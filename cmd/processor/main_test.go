package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/tidwall/gjson"

	"github.com/jonny/go-demo/internal/jobs"
	"github.com/jonny/go-demo/internal/queue"
)

var (
	testAMQPURL     string
	testDatabaseURL string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	rabbit, err := rabbitmq.Run(ctx, "rabbitmq:4-management-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting RabbitMQ test container:", err)
		os.Exit(1)
	}
	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("jobs"), postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"), postgres.BasicWaitStrategies())
	if err != nil {
		_ = testcontainers.TerminateContainer(rabbit)
		fmt.Fprintln(os.Stderr, "starting Postgres test container:", err)
		os.Exit(1)
	}
	testAMQPURL, err = rabbit.AmqpURL(ctx)
	if err == nil {
		testDatabaseURL, err = postgresContainer.ConnectionString(ctx, "sslmode=disable")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "getting container connection details:", err)
		_ = testcontainers.TerminateContainer(rabbit)
		_ = testcontainers.TerminateContainer(postgresContainer)
		os.Exit(1)
	}

	code := m.Run()
	if testcontainers.TerminateContainer(rabbit) != nil {
		code = 1
	}
	if testcontainers.TerminateContainer(postgresContainer) != nil {
		code = 1
	}
	os.Exit(code)
}

func discardTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestConfigFromEnvUsesComposeHostDefaults(t *testing.T) {
	for _, name := range []string{"AMQP_URL", "DATABASE_URL", "WORKER_COUNT", "RETRY_BUDGET"} {
		t.Setenv(name, "")
	}
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AMQPURL != defaultAMQPURL || cfg.DatabaseURL != defaultDatabaseURL || cfg.Workers != 5 || cfg.Retries != 2 {
		t.Errorf("configFromEnv() = %+v, want host defaults", cfg)
	}
}

func TestConfigFromEnvRejectsInvalidCounts(t *testing.T) {
	for _, tt := range []struct{ name, value string }{{"WORKER_COUNT", "0"}, {"WORKER_COUNT", "many"}, {"RETRY_BUDGET", "-1"}} {
		t.Run(tt.name+"="+tt.value, func(t *testing.T) {
			t.Setenv(tt.name, tt.value)
			if _, err := configFromEnv(); err == nil {
				t.Fatal("configFromEnv() succeeded, want an error")
			}
		})
	}
}

func TestRunPersistsExactlyTheSeededJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RabbitMQ/Postgres processor test in short mode")
	}
	const seed = 42
	schemaName := "test_run_persists"
	db, err := sqlx.Connect("pgx", testDatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP SCHEMA IF EXISTS ` + schemaName + ` CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE SCHEMA ` + schemaName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP SCHEMA ` + schemaName + ` CASCADE`) })

	u, _ := url.Parse(testDatabaseURL)
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topology := queue.Topology{
		JobsQueue: "cmd-jobs-" + suffix, DeadLetterExchange: "cmd-dead-" + suffix,
		DeadLetterQueue: "cmd-dead-" + suffix,
	}
	cfg := config{AMQPURL: testAMQPURL, DatabaseURL: u.String(), Workers: 5, Retries: 2, Seed: seed, Topology: topology}

	want := make([]string, 0)
	for _, raw := range queue.NewProducer("", topology, seed, 0).Payloads(messageCount) {
		if gjson.GetBytes(raw, "eventType").String() == jobs.EventJob {
			want = append(want, gjson.GetBytes(raw, "eventId").String())
		}
	}
	slices.Sort(want)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	go func() { done <- run(ctx, log, cfg) }()

	deadline := time.Now().Add(30 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		err = db.Select(&got, `SELECT event_id FROM `+schemaName+`.transient_jobs ORDER BY event_id`)
		if err == nil && slices.Equal(got, want) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !slices.Equal(got, want) {
		cancel()
		t.Fatalf("persisted ids = %v, want %v (last query error: %v)", got, want, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not shut down after cancellation")
	}
	conn, err := amqp.Dial(testAMQPURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	deadQueue, err := ch.QueueInspect(topology.DeadLetterQueue)
	if err != nil {
		t.Fatal(err)
	}
	if deadQueue.Messages != 0 {
		t.Errorf("healthy run left %d messages in the dead-letter queue, want 0", deadQueue.Messages)
	}
	if strings.Contains(logs.String(), "dead letter") {
		t.Errorf("healthy run consumed a dead letter: %s", logs.String())
	}
}

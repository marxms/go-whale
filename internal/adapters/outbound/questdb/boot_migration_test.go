package questdb

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// relatorioParcial is the outcome measured in production: the pass died in the middle and had
// already migrated candles.
func relatorioParcial() SoleCopyMigrationReport {
	return SoleCopyMigrationReport{
		Applied: true,
		Commits: 3,
		Cells: []SoleCopyCell{{
			Timeframe: "1d", Keys: 500, KeysWithSoleCopy: 297, Candles: 60_000,
		}},
	}
}

// migracaoEncenada returns a pass that answers the given sequence, one answer per call, and
// then repeats the last one.
func migracaoEncenada(passadas ...SoleCopyMigrationReport) (SoleCopyMigrationRun, *int) {
	calls := 0
	run := func(context.Context) (SoleCopyMigrationReport, error) {
		i := calls
		calls++
		if i >= len(passadas) {
			i = len(passadas) - 1
		}
		return passadas[i], nil
	}
	return run, &calls
}

// relatorioVazio is the re-read that finds nothing below the durable boundary.
func relatorioVazio() SoleCopyMigrationReport {
	return SoleCopyMigrationReport{Applied: true, Cells: []SoleCopyCell{{Timeframe: "1d", Keys: 500}}}
}

func TestRunBootSoleCopyMigration_PassadaCompletaAutorizaOSegundoEstagio(t *testing.T) {
	run, calls := migracaoEncenada(relatorioParcial(), relatorioVazio())

	outcome := RunBootSoleCopyMigration(context.Background(), run)

	if *calls == 0 {
		t.Fatal("stage 1 did not run the pass")
	}
	if outcome != BootMigrationComplete {
		t.Fatalf("outcome = %v, wanted complete", outcome)
	}
}

func TestRunBootSoleCopyMigration_OTerminoVemDoDadoENaoDeSinalizador(t *testing.T) {
	run, calls := migracaoEncenada(
		relatorioParcial(),
		relatorioParcial(),
		relatorioVazio(),
	)

	if outcome := RunBootSoleCopyMigration(context.Background(), run); outcome != BootMigrationComplete {
		t.Fatalf("outcome = %v, wanted complete", outcome)
	}
	if *calls != 3 {
		t.Fatalf("%d pass(es) executed, wanted 3", *calls)
	}
}

func TestRunBootSoleCopyMigration_NenhumEstadoPersistidoPulaOTrabalho(t *testing.T) {
	run, calls := migracaoEncenada(relatorioVazio())

	RunBootSoleCopyMigration(context.Background(), run)
	first := *calls
	RunBootSoleCopyMigration(context.Background(), run)

	if *calls <= first {
		t.Fatal("the second boot did not re-read the boundary — some persisted mark skipped the work")
	}
}

func TestRunBootSoleCopyMigration_NaoConvergenciaEhIncompleta(t *testing.T) {
	run, calls := migracaoEncenada(relatorioParcial())

	if outcome := RunBootSoleCopyMigration(context.Background(), run); outcome != BootMigrationIncomplete {
		t.Fatalf("outcome = %v, wanted INCOMPLETE", outcome)
	}
	if *calls != BootMigrationMaxPasses {
		t.Fatalf("%d pass(es), wanted the ceiling of %d", *calls, BootMigrationMaxPasses)
	}
}

func TestRunBootSoleCopyMigration_AbortoEhIncompletoEPublicaOParcial(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	outcome := RunBootSoleCopyMigration(context.Background(),
		func(context.Context) (SoleCopyMigrationReport, error) {
			return relatorioParcial(), errors.New("migration: read BTC/USDT 1d: connection reset")
		}, log)

	if outcome != BootMigrationIncomplete {
		t.Fatalf("outcome = %v, wanted INCOMPLETE", outcome)
	}

	var lines string
	for _, record := range sink.Records() {
		lines += record.Text() + "\n"
	}
	if !strings.Contains(lines, "60000") {
		t.Errorf("the migrated partial was not published on the abort outcome: %s", lines)
	}
	if !strings.Contains(lines, "aborted") {
		t.Errorf("the abort was not declared: %s", lines)
	}
}

func TestRunBootSoleCopyMigration_SemTierDuravelEhNaoAplicavel(t *testing.T) {
	if outcome := RunBootSoleCopyMigration(context.Background(), nil); outcome != BootMigrationNotApplicable {
		t.Fatalf("outcome = %v, wanted not applicable", outcome)
	}
}

func TestBootMigrationBudget_NaoEhOOrcamentoDoEsquema(t *testing.T) {
	if BootMigrationBudget < 30*time.Minute {
		t.Fatalf("budget = %v — the declared ceiling of the pass is 30m", BootMigrationBudget)
	}
	if ratio := BootMigrationBudget / ColdTierBudget; ratio < 180 {
		t.Fatalf("the migration budget is only %d× the schema budget — the measured ratio is 180×", ratio)
	}
}

func TestBootMigrationContext_TemPrazoProprio(t *testing.T) {
	ctx, finish := BootMigrationContext()
	defer finish()

	deadline, has := ctx.Deadline()
	if !has {
		t.Fatal("the migration context has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= ColdTierBudget || remaining > BootMigrationBudget {
		t.Fatalf("remaining = %v — wanted (%v, %v]", remaining, ColdTierBudget, BootMigrationBudget)
	}
}

func TestBootMigrationContext_EhCanceladoPorSinalDeTermino(t *testing.T) {
	ctx, finish := BootMigrationContext()
	defer finish()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM to the process itself: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM did not cancel the migration context")
	}
}

func TestRunBootSoleCopyMigration_SinalEntrePassadasInterrompe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	run := func(context.Context) (SoleCopyMigrationReport, error) {
		calls++
		cancel() // the signal arrives DURING the first pass
		return relatorioParcial(), nil
	}

	outcome := RunBootSoleCopyMigration(ctx, run)
	cancel()

	if outcome != BootMigrationIncomplete {
		t.Fatalf("outcome = %v, wanted INCOMPLETE", outcome)
	}
	if calls != 1 {
		t.Fatalf("%d pass(es) — the second was emitted after cancellation", calls)
	}
}

func TestBootMigrationCeiling_CobreACopiaUnicaMedida(t *testing.T) {
	const measuredSoleCopy = 95_811 + 33_379 // measured in production

	ceiling := BootMigrationMaxPasses * SoleCopyFlushThreshold
	if ceiling < measuredSoleCopy {
		t.Fatalf("ceiling per boot = %d candle(s), below the measured %d", ceiling, measuredSoleCopy)
	}
	if requiredPasses := (measuredSoleCopy / SoleCopyFlushThreshold) + 1; BootMigrationMaxPasses != requiredPasses+1 {
		t.Fatalf("BootMigrationMaxPasses = %d — the measured %d candles require %d pass(es) plus the re-read",
			BootMigrationMaxPasses, measuredSoleCopy, requiredPasses)
	}
}

func TestRunBootSoleCopyMigration_PassadaCortadaPeloTetoNaoEhConcluida(t *testing.T) {
	truncated := relatorioVazio()
	truncated.Truncated = true

	run, _ := migracaoEncenada(truncated)

	if outcome := RunBootSoleCopyMigration(context.Background(), run); outcome != BootMigrationIncomplete {
		t.Fatalf("outcome = %v — a pass cut by the ceiling was read as completion", outcome)
	}
}

func TestRunBootSoleCopyMigration_TetoPorArranqueRemeteAoArranqueSeguinte(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	truncated := relatorioParcial()
	truncated.Truncated = true
	run, calls := migracaoEncenada(truncated)

	if outcome := RunBootSoleCopyMigration(context.Background(), run, log); outcome != BootMigrationIncomplete {
		t.Fatalf("outcome = %v, wanted INCOMPLETE", outcome)
	}
	if *calls != BootMigrationMaxPasses {
		t.Fatalf("%d pass(es), wanted the ceiling of %d", *calls, BootMigrationMaxPasses)
	}

	var lines string
	for _, record := range sink.Records() {
		lines += record.Text() + "\n"
	}
	if !strings.Contains(lines, "next boot resumes") {
		t.Errorf("the ceiling outcome does not say the next boot resumes: %s", lines)
	}
	if !strings.Contains(lines, "no license") {
		t.Errorf("the ceiling outcome does not declare retention withheld: %s", lines)
	}
}

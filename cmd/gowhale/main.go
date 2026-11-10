// Command gowhale runs the streamer: `serve` starts ingestion and `doctor` checks the boot
// preconditions without opening a port or exiting through an internal package.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/marxms/go-whale/internal/app"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
	"github.com/marxms/go-whale/internal/platform/doctor"
	logger "github.com/marxms/go-whale/internal/platform/logging"
	"github.com/marxms/go-whale/internal/platform/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "doctor":
			return doctor.Run(args[1:], stdout, stderr)
		case "serve":
			args = args[1:]
		case "version":
			_, _ = fmt.Fprintln(stdout, version.Current().String())
			return 0
		}
	}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		_, _ = fmt.Fprintf(stderr, "unknown command %q; use serve, doctor or version\n", args[0])
		return 2
	}

	fs := flag.NewFlagSet("gowhale", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configFile := fs.String("config", "", "path to the YAML configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	return serve(*configFile, stderr)
}

func serve(configFile string, stderr io.Writer) int {
	cfg, err := config.Load(configFile)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config: %v\n", err)
		return 1
	}
	logCfg, err := logger.ConfigFromProcess()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "log: %v\n", err)
		return 1
	}
	// The config file (or its environment overrides) is the authoritative source; the
	// process environment above supplies the defaults for environment-only mode.
	if cfg.LogLevel != "" {
		logCfg.Level = logger.Level(cfg.LogLevel)
	}
	if cfg.LogFormat != "" {
		logCfg.Format = cfg.LogFormat
	}
	processLog, err := logger.New(logCfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "log: %v\n", err)
		return 1
	}

	tfReg := candle.NewTimeframeRegistry(cfg.CandleEngine.Timeframes)
	application, err := app.NewApp(cfg, tfReg, processLog)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "app: %v\n", err)
		return 1
	}
	if err := application.Run(context.Background()); err != nil {
		_, _ = fmt.Fprintf(stderr, "run: %v\n", err)
		return 1
	}
	return 0
}

package main

import (
	"flag"
	"github.com/bitwrap/dagstore"
	_db "github.com/bitwrap/dagstore/db"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ledisdb/ledisdb/server"
)

var (
	Storage    = &_db.DB{}
	app        *server.App
	addr       = "0.0.0.0:6380"
	usePprof   = false
	pprofPort  = 6060
	slaveof    = ""
	readonly   = false
	rpl        = false
	rplSync    = false
	ttlCheck   = 0
	databases  = 0
	dbName     = ""
	dataDir    = "/tmp"
	configFile = ""
)

func main() {
	ParseFlags()
	InitServer()
	Serve()
}

func InitServer(configFile ...string) {
	cfg := _db.Config(configFile...)
	var err error

	if err != nil {
		panic(err)
	}

	if len(addr) > 0 {
		cfg.Addr = addr
	}

	if len(dataDir) > 0 {
		cfg.DataDir = dataDir
	}

	if len(dbName) > 0 {
		cfg.DBName = dbName
	}

	if databases > 0 {
		cfg.Databases = databases
	}

	// check bool //flag, use it.
	for _, arg := range os.Args {
		arg := strings.ToLower(arg)
		switch arg {
		case "-rpl", "-rpl=true", "-rpl=false":
			cfg.UseReplication = rpl
		case "-readonly", "-readonly=true", "-readonly=false":
			cfg.Readonly = readonly
		case "-rpl_sync", "-rpl_sync=true", "-rpl_sync=false":
			cfg.Replication.Sync = rplSync
		}
	}

	if len(slaveof) > 0 {
		cfg.SlaveOf = slaveof
		cfg.Readonly = true
		cfg.UseReplication = true
	}

	if ttlCheck > 0 {
		cfg.TTLCheckInterval = ttlCheck
	}

	app, err = server.NewApp(cfg)
	if err != nil {
		panic(err)
	}
	ldb, err := app.Ledis().Select(0) // REVIEW: should we support > 1 db index?
	if err != nil {
		panic(err)
	}
	Storage.DB = ldb // set runtime storage.go
}

func ParseFlags() {
	flag.Parse()
	addr = *flag.String("addr", "", "ledisdb listen address")
	usePprof = *flag.Bool("pprof", false, "enable pprof")
	pprofPort = *flag.Int("pprof_port", 6060, "pprof http port")
	slaveof = *flag.String("slaveof", "", "make the server a slave of another instance")
	readonly = *flag.Bool("readonly", false, "set readonly mode, slave server is always readonly")
	rpl = *flag.Bool("rpl", false, "enable replication or not, slave server is always enabled")
	rplSync = *flag.Bool("rpl_sync", false, "enable sync replication or not")
	ttlCheck = *flag.Int("ttl_check", 0, "TTL check interval")
	databases = *flag.Int("databases", 0, "ledisdb maximum database number")
	dbName = *flag.String("db_name", "", "select a db to use, it will overwrite the config's db name")
	dataDir = *flag.String("data_dir", "", "ledisdb base data dir")
	configFile = *flag.String("config", "", "ledisdb config file")
}

func Serve() {
	go dagstore.ProcessEpochs()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	/*
		if *usePprof {
			go func() {
				log.Println(http.ListenAndServe(fmt.Sprintf(":%d", *pprofPort), nil))
			}()
		}
	*/
	go app.Run()
	<-sc
	print("server is closing...")
	app.Close()
	println("closed")
}

package db

import (
	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"
)

const (
	Version = "v1"
	Prefix  = "/" + Version + "/"
	Welds   = Prefix + "weld/"
)

type DB struct {
	*ledis.DB
}

func Config(configFile ...string) (cfg *config.Config) {
	var err error
	if len(configFile) != 1 {
		println("no config set, using default config")
		cfg = config.NewConfigDefault()
	} else {
		cfg, err = config.NewConfigWithFile(configFile[0])
	}
	if err != nil {
		panic(err)
	}
	return cfg
}

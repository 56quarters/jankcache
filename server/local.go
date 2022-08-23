package server

/*
	r, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters: 100000,
			MaxCost:     10000,
			BufferItems: 64,
			Metrics:     true,
		},
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create cache: %s", err)
		os.Exit(1)
	}

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	e := proto.Encoder{}
	p := proto.Parser{}
	c := cache.NewAdapter(r, l)

	h := server.NewHandler(p, e, c)

	for {
		err := h.Handle(os.Stdin, os.Stdout)
		if err != nil {
			level.Warn(l).Log("msg", "error while reading input", "err", err)
		}
	}
*/

### Emacs build
Start the Erlang shell while pointing it to the `/ebin` folder with:
```erlang
erl -pa ebin
```
To compile all the sources execute this command from the shell:
```bash
make:all([{outdir, './ebin'}]).
```


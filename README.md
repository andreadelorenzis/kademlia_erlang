### How to build
Start the Erlang shell while pointing it to the `/ebin` folder with:
```erlang
erl -pa ebin
```
To compile all the sources execute this command from the shell in the root directory:
```bash
make:all([{outdir, './ebin'}]).
```

### Emacs build
Start the Erlang shell while pointing it to the `/ebin` folder with:
```erlang
erl -pa ebin
```
To compile all the sources execute this command from the shell:
```bash
make:all([{outdir, './ebin'}]).
```

ToDO:
[] far funzionare refresh
[] misurare tempi di lookup medi con diversi valori per il numero di nodi (10, 100, 1000, ecc..). 
   Far stabilizzare prima la rete e poi effettuare qualche lookup di valori a mano, su nodi diversi dai K nodi sui cui è stato salvato. 
[] Fare stesse misurazioni per join di un nodo
[] Fare stesse misurazioni per lookup con fallimento di fino a K-1 nodi (plottare il grafico con più
   linee in funzione del numero di nodi).# kademlia_erlang_impl

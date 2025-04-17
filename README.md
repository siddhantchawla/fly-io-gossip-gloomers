# fly-io-gossip-gloomers
This repository contains my soltuions for the fly.io 's Distributed Systems Challenge - "Gossip Gloomers". Find the challenge here: https://fly.io/dist-sys/ 


Codesspaces setup: 

- sudo apt update
- sudo apt install graphviz
- sudo apt install gnuplot
- curl -L -o maelstrom.tar.bz2 https://github.com/jepsen-io/maelstrom/releases/latest/download/maelstrom.tar.bz2
- tar -xvjf maelstrom.tar.bz2
- echo 'export PATH="$PATH:~/workspaces/fly-io-gossip-gloomers/maelstrom/maelstorm"' >> ~/.bashrc
- source ~/.bashrc
- export GOBIN=$HOME/go/bin

For Compilation: 
- go get github.com/jepsen-io/maelstrom/demo/go
- go install .


help:
	@cat Makefile

EXE=dispenser-server

build:
	stack build

clean:
	stack clean

dist-clean:
	\rm -rf .stack-work

hlint:
	stack exec hlint .

run:
	stack exec $(STACK_ARGS) -- $(EXE) $(ARGS)

setup:
	stack setup

test:
	stack test

watch:
	stack build --fast --file-watch

watch-test:
	stack test --fast --file-watch

b: build
hl: hlint
i: install
r: run
s: setup
w: watch
t: test
wt: watch-test

.PHONY: test

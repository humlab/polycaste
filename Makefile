.DEFAULT_GOAL=lint
SOURCE_FOLDERS=polycaste tests
PACKAGE_FOLDER=polycaste
SHELL := /bin/bash

word-dot = $(word $2,$(subst ., ,$1))

RUN_TIMESTAMP := $(shell /bin/date "+%Y-%m-%d-%H%M%S")

JULIA_HOME := $(HOME)/bin/julia
JULIA_VERSION := 1.6.1
JULIA_MINOR := $(call word-dot,$(JULIA_VERSION),1).$(call word-dot,$(JULIA_VERSION),2)
JULIA_TAR := julia-$(JULIA_VERSION)-linux-x86_64.tar.gz
JULIA_FOLDER := $(basename $(basename $(JULIA_TAR)))

#JULIA_DIR := $(shell $(JULIA) -e 'print(dirname(Sys.BINDIR))')

.ONESHELL: install-julia
install-julia:
	@cd $(HOME)/bin && rm -f ./julia
	@wget https://julialang-s3.julialang.org/bin/linux/x64/$(JULIA_MINOR)/$(JULIA_TAR)
	@tar zxvf $(JULIA_TAR)
	@ln -s $(HOME)/bin/julia-$(JULIA_VERSION) $(HOME)/bin/julia
	@rm -f $(HOME)/bin/$(JULIA_TAR)
	@if ! echo "$$PATH" | /bin/grep -Eq "(^|:)${JULIA_HOME}/bin($|:)" ; then
		echo  >> ~/.bashrc
		echo "if [ -L \$$HOME/bin/julia ]; then " >> ~/.bashrc
		echo "    export PATH=\$$HOME/bin/julia/bin:\$$PATH"  >> ~/.bashrc
		echo "fi" >> ~/.bashrc
	 fi

clean-cache:
	@rm -rf ~/.julia/precompiled/Polycaste

build-pycall:
	julia --project=. -e 'ENV["PYTHON"]=""; using Pkg; Pkg.build("PyCall")'

corpus:
	julia --project=. --threads=10 src/tasks/corpus.jl

.PHONY: test build resolve lint coverage

test:
	julia --project=. -e 'using Pkg; Pkg.test(coverage=false)'

build:
	julia --project=. -e 'using Pkg; Pkg.build()'

precompile:
	julia --project=. -e 'using Pkg; Pkg.precompile()'

resolve:
	julia --project=. -e 'using Pkg; Pkg.resolve()'

coverage:
	julia -e 'cd(Pkg.dir("${PROJECT_NAME}")); Pkg.add("${PROJECT_NAME}"); using Coverage; @show get_summary(Coverage.process_folder())'

lint:
	julia -e 'using Lint; for i in readdir("src") if ismatch(r".*\.jl$$", i) lintfile(string("src/", i)) end end'
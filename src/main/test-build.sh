#rm wc.so
#rm mr-out*


rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*
cd ../
# make sure software is freshly built.
# (cd ../../mrapps && go clean)
# (cd .. && go clean)
# (cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
# (cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
# (cd .. && go build $RACE mrcoordinator.go) || exit 1

go build -race -buildmode=plugin ../mrapps/wc.go

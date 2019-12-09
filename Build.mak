export ASSERT_ON_STOMPING_PREVENTION=1

override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
override DFLAGS += -w -de

$B/fakedmq: $C/src/fakedmq/main.d

all += $B/fakedmq $B/neotest

$O/test-fakedmq: $B/fakedmq

$B/dmqapp: $C/src/dummydmqapp/main.d

$O/test-dmqrestart: $B/dmqapp
$O/test-dmqrestart: override LDFLAGS += -llzo2 -lebtree  -lrt -lpcre

$O/test-dmqhelpers: $B/dmqapp
$O/test-dmqhelpers: override LDFLAGS += -llzo2 -lebtree -lrt -lpcre

run-test:
	$O/test-fakedmq

debug-test:
	gdb $O/test-fakedmq

$B/neotest: override LDFLAGS += -lebtree -llzo2 -lrt -lgcrypt -lglib-2.0 -lgpg-error
$B/neotest: neotest/main.d
neotest: $B/neotest

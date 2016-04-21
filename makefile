CC=gcc
CFLAGS=-pthread -O0 -m64 -c -Wall -fmessage-length=0

all: test_assign4

dberror.o: dberror.c
	$(CC) $(CFLAGS) dberror.c

storage_mgr.o: storage_mgr.c
	$(CC) $(CFLAGS) storage_mgr.c

buffer_mgr.o: buffer_mgr.c
	$(CC) $(CFLAGS) buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	$(CC) $(CFLAGS) buffer_mgr_stat.c

rm_serializer.o: rm_serializer.c
	$(CC) $(CFLAGS) rm_serializer.c

record_mgr.o: record_mgr.c
	$(CC) $(CFLAGS) record_mgr.c

expr.o: expr.c
	$(CC) $(CFLAGS) expr.c

btree_mgr.o: btree_mgr.c
	$(CC) $(CFLAGS) btree_mgr.c

test_assign4_1.o: test_assign4_1.c
	$(CC) $(CFLAGS) test_assign4_1.c

test_expr.o: test_expr.c
	$(CC) $(CFLAGS) test_expr.c

test_assign4: dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o rm_serializer.o record_mgr.o expr.o btree_mgr.o test_assign4_1.o
	$(CC) dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o rm_serializer.o record_mgr.o expr.o btree_mgr.o test_assign4_1.o -o test_assign4

test_expr: dberror.o storage_mgr.o buffer_mgr_page_op.o buffer_mgr_pool_op.o buffer_mgr_stat.o rm_serializer.o record_mgr_serde.o expr.o record_mgr_op.o record_mgr_table_op.o record_mgr_record_op.o test_expr.o
	$(CC) dberror.o storage_mgr.o buffer_mgr_page_op.o buffer_mgr_pool_op.o buffer_mgr_stat.o rm_serializer.o record_mgr_serde.o expr.o record_mgr_op.o record_mgr_table_op.o record_mgr_record_op.o test_expr.o -o test_expr

clean:
	rm *.o test_assign4

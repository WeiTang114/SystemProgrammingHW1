ifdef DEBUG
	CFLAGS = -DDEBUG
else
	CFLAGS = 
endif

all: server.c
	gcc server.c -o write_server $(CFLAGS)
	gcc server.c -D READ_SERVER -o read_server $(CFLAGS)

clean:
	rm -f read_server write_server

#ifndef EXCEPTION_BUFFER_H
#define EXCEPTION_BUFFER_H

#include <setjmp.h>

typedef struct exception_buffer {
	jmp_buf state;
	int code;
	char *msg;
} exception_buffer;

extern exception_buffer *eb_create(void);
extern void eb_destroy( exception_buffer *eb );

/* != 0 on when we return to the savepoint */
#define eb_savepoint(eb) (setjmp(eb->state))
extern void eb_error( exception_buffer *eb, char *msg, int val );

#endif /* EXCEPTION_BUFFER_H */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
// build a sample REPL(read–execute–print loop)
typedef struct {
    char* buffer;
    size_t buffer_length; // unsigned int
    ssize_t input_length; // 数据类型用来表示可以被执行读写操作的数据块的大小, signed
} InputBuffer;

// 新建初始化
InputBuffer* new_input_buffer() {
    InputBuffer* inputBuffer = malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;

    return inputBuffer;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer* input_Buffer) {
    ssize_t bytes_read = getline(&(input_Buffer->buffer), &(input_Buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // Ignore trailing newline
    input_Buffer->input_length = bytes_read - 1;
    input_Buffer->buffer[bytes_read - 1] = 0;
    // 因为getline会将\n读取进去，现在长度为真实长度+1，将最后一位舍去然后得到结尾0。
    // 最后位存放字符串结束符'\0'
}

void close_input_buffer(InputBuffer* input_Buffer) {
    free(input_Buffer->buffer);
    free(input_Buffer);
}

int main(int argc, char* argv[]) {
    InputBuffer* input_Buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_Buffer);

        if (strcmp(input_Buffer->buffer, ".exit") == 0) {
            close_input_buffer(input_Buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s'.\n", input_Buffer->buffer);
        }
    }
    return 0;
}

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

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
} Statement;

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

MetaCommandResult do_meta_command(InputBuffer* inputBuffer) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0)
        exit(EXIT_SUCCESS);
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer* inputBuffer, Statement* statement) {
    // 把 str1 和 str2 进行比较，最多比较前 n 个字节。
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            printf("This is where we would do an insert.\n");
            break;
        case (STATEMENT_SELECT):
            printf("This is where we would do a select.\n");
            break;
    }
}

int main(int argc, char* argv[]) {
    InputBuffer* input_Buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_Buffer);

        if (input_Buffer->buffer[0] == '.') {
            switch (do_meta_command(input_Buffer)) {
                // .exut call metacommand
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_Buffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_Buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_Buffer->buffer);
                continue;
        }
        execute_statement(&statement);
        printf("Executed.\n");
    }
    return 0;
}

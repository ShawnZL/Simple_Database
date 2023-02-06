#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
// build a sample REPL(read–execute–print loop)
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100

// 也表示node
typedef struct {
    int file_descripter;
    uint32_t file_length;
    uint32_t num_pages; // 每一个节点对应的页书
    void* pages[TABLE_MAX_PAGES];
}Pager;

// 指向页的数组指针
typedef struct {
    uint32_t root_page_num; // B树父节点的对于的page数量
    Pager* pager;
} Table;

// Row 代表一行数据，需要我们进行存储的数据
typedef struct {
    uint32_t  id;
    char username[COLUMN_USERNAME_SIZE + 1]; // string 以空字符结尾，所以需要加上这个！
    char email[COLUMN_EMAIL_SIZE + 1];
}Row;

typedef struct {
    char* buffer;
    size_t buffer_length; // unsigned int
    ssize_t input_length; // 数据类型用来表示可以被执行读写操作的数据块的大小, signed
} InputBuffer;

typedef struct {
    Table* table;
    uint32_t page_num; // 页码
    uint32_t cell_num; // key/value 对
    bool end_of_table; // Indicates a position one past the last element
}Cursor;
//the row is the way we can identify a location.

typedef enum {
    NODE_INTERNAL, NODE_LEAF
}NodeType;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,// 参数数量不够
    PREPARE_NEGATIVE_ID, // 负数ID编号
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_STRING_TOO_LONG // String超长
} PrepareResult;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;


/*
+ * Common Node Header Layout
+ */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
+ * Leaf Node Body Layout
+ */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

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

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

//公共头部偏移量之后的东西，就是数量，返回数量指针
uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
// 叶节点头偏移+key*value之后偏移，需要添加位置的偏移量
uint32_t* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// 得到node的类型，是root还是其他类型
NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;// 类型转换
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}


void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    *leaf_node_num_cells(node) = 0;
}

// 在pager中新建一个node(page)，给node开辟空间，之后刷新到磁盘中
// page_num由调用给出，一般都用在Table->root_page_num。这样就是直接后插上
void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    // Cache miss. Allocate memory and load from file
    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        //文件需要的页面数，本次是1
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // we might save a partial page at the end of the file
        // 我们可能会在文件末尾保存部分页面
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        // 当前分配的page_num 比num_pages计算来的要小，一般存在情况是初始化过程
        if (page_num <= num_pages) {
            lseek(pager->file_descripter, page_num * PAGE_SIZE, SEEK_SET); //设置文件偏移量
            ssize_t bytes_read = read(pager->file_descripter, page, PAGE_SIZE); //将文件转移进入page中，目前都在Cache中
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        // 不存在上述问题，直接使用pages进行链接！
        pager->pages[page_num] = page;
        // 更新节点的孩子数量。page_num是最后插入的，所以page_num+1
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}


Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

// 使用二分法进行查找
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        else if (key < key_at_index) {
            one_past_max_index = index;
        }
        else {
            min_index = index + 1;
        }
    }
    cursor->cell_num = min_index;
    return cursor;
}

/*
 Return the position of the given key.
 If the key is not present, return the position
 where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    }
    else {
        printf("Need to implement searching an internal node\n");
        exit(EXIT_FAILURE);
    }
}

// 从file中将page打开，恢复出来。
Pager* pager_open(const char* filename) {
    int fd = open(filename, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR);// 第三个参数用来确定权限
    // Read/Write mode,        Create file if it does not exist
    // User write permission,  User read permission
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END); // lseek 的此用法是返回当前的偏移量
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descripter = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }
    return pager;
}

// 将页写入磁盘
// void pager_flush(Pager* pager, uint32_t page_num, uint32_t) {
// old
// P8new 每个节点都将只占用一页，即使它未满。 这意味着我们的分页器不再需要支持读/写部分页面。！！！
void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descripter, page_num * PAGE_SIZE, SEEK_SET);
    // 设定偏移量，如果设置不成功，则返回错误
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    // 写入
    // ssize_t bytes_written = write(pager->file_descripter, pager->pages[page_num], size);
    ssize_t bytes_written = write(pager->file_descripter, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

// initialize the table
// new_table rename db_open
//opening the database file, initializing a pager data structure, initializing a table data structure
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    // 记录文件长度，然后除row大小可以得到总行数。

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}


// 将source信息放置在目标destination位置，同时使用偏移量
// 目的是将页面紧凑
// memcpy 从存储区 str2 复制 n 个字节到存储区 str1
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);

    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

// 从destination复制文件进入source返回过去
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    // 返回节点，没有的话新建
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full 未实现Btree节点分开
        printf("Need to implement splitting a leaf node.\n");
        exit(EXIT_FAILURE);
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        // 从后向前复制
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                   LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
    }

//将Cache刷新进入磁盘中，关闭数据库 file，然后释放内存
void db_close(Table* table) {
    Pager* pager = table->pager; // 全部page表格

    for (uint32_t i = 0; i < pager->num_pages; ++i) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descripter);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

// 打印内容
void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void* node) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

MetaCommandResult do_meta_command(InputBuffer* inputBuffer, Table* table) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else if (strcmp(inputBuffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    }
    else if (strcmp(inputBuffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

// 判断插入string是否超出界限
PrepareResult prepare_Insert(InputBuffer* inputBuffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    // strtok分割以谁结尾，第一个使用完毕后，结尾指针会存在strtok函数中
    char* keyword = strtok(inputBuffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL)
        return PREPARE_SYNTAX_ERROR;
    // 这没有判断id是否重复！
    int id = atoi(id_string);
    if (id < 0) { // id 小于0
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

// 执行命令工作，insert和select两个东西需要进行存储
// 但是这里前期有一个问题就是没有添加相应的判定str字节大小，string超出限制将无法进行存储，造成buffer溢出
PrepareResult prepare_statement(InputBuffer* inputBuffer, Statement* statement) {
    // 把 str1 和 str2 进行比较，最多比较前 n 个字节。
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        return prepare_Insert(inputBuffer, statement);
    }
    if (strncmp(inputBuffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

// old 确定在哪里开始写页。目的是找到那一页。
// where to read/write in memory for a particular row
/*void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}*/
// new
void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

// cursor 移动
void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        cursor->end_of_table = true;
    }
}


// 执行插入
ExecuteResult execute_insert(Statement* statement, Table* table) {
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    // Cursor* cursor = table_end(table); 不再需要插入到最后一页
    // table 是表，现有位置，然后根据table存储的num_rows计算应该插入到那一页
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);
    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    return EXECUTE_SUCCESS;
}

// 执行选择
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor = table_start(table);
    /*for (uint32_t i = 0; i < table->num_rows; i++) {
        // 将table内容的复制到row中，然后展示出来
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }*/
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

// 执行结果
ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    //char* filename = "mydb.db";debug
    Table* table = db_open(filename);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        // .statement代表.命令，没有就是插入数据等其他任务
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("string is too long.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
        }
        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate key.\n");
        }
    }
    return 0;
}

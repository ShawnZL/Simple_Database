#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
// build a sample REPL(read–execute–print loop)

typedef struct {
    char* buffer;
    size_t buffer_length; // unsigned int
    ssize_t input_length; // 数据类型用来表示可以被执行读写操作的数据块的大小, signed
} InputBuffer;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY
} ExecuteResult;

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
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
// Row 代表一行数据，需要我们进行存储的数据
typedef struct {
    uint32_t  id;
    char username[COLUMN_USERNAME_SIZE + 1]; // string 以空字符结尾，所以需要加上这个！
    char email[COLUMN_EMAIL_SIZE + 1];
}Row;

typedef struct {
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

#define TABLE_MAX_PAGES 100

// 也表示node
typedef struct {
    int file_descripter;
    uint32_t file_length; // 全部文件长度
    uint32_t num_pages; // 当前Pager对应的pages里页面的数量
    void* pages[TABLE_MAX_PAGES]; // 指向各个page的指针数组
}Pager;

// 指向页的数组指针
typedef struct {
    uint32_t root_page_num; // root节点所在位置
    // which page store the content
    Pager* pager;
} Table;


typedef struct {
    Table* table;
    uint32_t page_num; // 页码
    uint32_t cell_num; // key/value 对
    bool end_of_table; // Indicates a position one past the last element
}Cursor;

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

typedef enum {
    NODE_INTERNAL, NODE_LEAF
}NodeType;

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
+ * Internal Node Header Layout
+ */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
        INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
+ * Internal Node Body Layout
+ */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
        INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;


/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
        LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
        LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;


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
// left node to get one more cell if N+1 is odd.
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// 得到node的类型，是root还是其他类型
NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;// 类型转换
}



// 寻找下一个节点所在的位置
uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

// reading and write to an internal node
// 得到的是关于num_keys内容的指针
uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}
// 得到的是right_child的指针
uint32_t* internal_node_right_child(void* node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}
// 对应位置cell_num对应的起点
uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    }
    else if (child_num == num_keys) { // 对应最大值，应该在右孩子中寻找
        return internal_node_right_child(node);
    }
    else {
        return internal_node_cell(node, child_num);
    }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

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

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

// 判断节点是否是root
bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

// 设置root节点的类型
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
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

// 使用二分法进行查找
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    cursor->end_of_table = false;

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
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor->cell_num = min_index;
    return cursor;
}
// 内部节点寻找key
// 寻找到包含key的leaf_node
uint32_t internal_node_find_child(void* node, uint32_t key) {
    /*
      Return the index of the child which should contain
      the given key.
    */
    uint32_t num_keys = *internal_node_num_keys(node);

    /*binary search*/
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; // there is one more child than key
    // 找到min_index锁定在page
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) { //比最大值要小
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    // 找到了在哪一page中有key
    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }
}

/*
 Return the position of the given key.
 If the key is not present, return the position
 where it should be inserted
 从根节点开始寻找节点
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);
    // 得到当前table指向的page
    if (get_node_type(root_node) == NODE_LEAF) {
        // 如果是叶节点，直接二分法开始在内部寻找节点
        return leaf_node_find(table, root_page_num, key);
    }
    else {
        // 如果是内部节点，则需要从内部节点再去寻找
        return internal_node_find(table, root_page_num, key);
    }
}
// 并添加一个搜索键 0（最小可能键）的新实现。 即使 key 0 在表中不存在，
// 该方法也会返回最低 id 的位置（最左边叶节点的开始）。
Cursor* table_start(Table* table) {
    Cursor* cursor =  table_find(table, 0);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
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
        set_node_root(root_node, true);
    }
    return table;
}


// 将source信息放置在目标destination位置，同时使用偏移量
// 目的是将页面紧凑
// memcpy 从存储区 str2 复制 n 个字节到存储区 str1
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    /*
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);*/
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// 从destination复制文件进入source返回过去
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// 得到最大的node节点，分情况讨论
// 对于内部节点，最大密钥始终是其右部。 对于叶节点，它是最大索引处的键在本身：
uint32_t get_node_max_key(void* node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}

/* Allocating New pages 分配新的page
 * Until we start recycling free pages, new pages will always
 * go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) {
    return pager->num_pages;
}

uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }

// 现在我们需要在父节点中找到受影响的单元格。 孩子不知道自己的页码，所以我们无法寻找。
// 但它确实知道自己的最大密钥，因此我们可以在父级中搜索该密钥。
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
    /* 前提是右孩子已经处理完毕，
     * 此处的右孩子表示的root中用来作为搜索条件的值，这个值只是ID索引，没有内容
     * 大于root的在右孩子中寻找，小于等于的在左孩子中寻找，
     * 最大值的寻找使用过get_node_max_key寻找的，同时存储的key(i)-value(page i-MaxValue)
     * Handle splitting the root.
     * Old root copied to new page, becomes left child.
     * Address of right child passed in.
     * Re-initialize root page to contain the new root node.
     * New root node points to two children.*/
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);// new page,从最后开始计算
    void* left_child = get_page(table->pager, left_child_page_num);
    // the old root is copied to the left child so we can reuse the root page:
    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    // 左右孩子的父指针需要初始化
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) = table->root_page_num;
}

// 分裂internal节点。
// Leaf Page满、internal Page满时，拆分Leaf Page，小于中间节点的记录放左边，
// 大于中间节点的记录放右边；拆分Index Page，原理同上，此时树的高度+1
// step0: add new key to internal and link the child, don't worry about the max, in the next step this will be done.
// step1: create new internal and get the right old parent,
// step2: create new internal(n+1) and join new child,
// step3: link the new internal and the root
void internal_node_split(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    /* 思考加入internal尚未更新的情况。
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index = internal_node_find_child(parent, child_max_key);
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t right_child_max_key = get_node_max_key(right_child);
    */
    void* old_node = get_page(table->pager, parent_page_num);// old
    // 建立新的internal节点
    uint32_t new_internal_page_num = get_unused_page_num(table->pager);
    void* new_internal_node = get_page(table->pager, new_internal_page_num);
    // 建立新的right部分
    uint32_t new_page_num = get_unused_page_num(table->pager); // 获取新page页面。作为右半部分
    void* new_node = get_page(table->pager, new_page_num); // 建立新的page
    // 找到目前internal里需要提升的key，也就是nums_keys / 2。
    uint32_t old_node_num_keys = *internal_node_num_keys(old_node);
    uint32_t med_key = old_node_num_keys / 2;
    for (int32_t i = old_node_num_keys; i >= 0; i--) {
        void* destination_node;
        // 确定节点的在左孩子还是右孩子
        if (i > med_key) {//大于左边计数
            destination_node = new_node; // new_node代表右孩子，相对来说大的
        } else {
            destination_node = old_node; // old_node代表左孩子，相对来说小的
        }

    }
    
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    /*
     * Add a new child/key pair to parent that corresponds to child*/
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;
    /* should be
    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        // 需要添加分离split internal node
        printf("Need to implement splitting internal node\n");
        exit(EXIT_FAILURE);
        //
        // internal_node_split(table, parent_page_num, child_page_num);
        // return;
    } */
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(table->pager, right_child_page_num);

    if (child_max_key > get_node_max_key(right_child)) {
        /* Replace right child */
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) =
                get_node_max_key(right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        /* Make room for the new cell */
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        // 需要添加分离split internal node
        printf("Need to implement splitting internal node\n");
        exit(EXIT_FAILURE);
        /*
        internal_node_split(table, parent_page_num, child_page_num);
        return;*/
    }
}

// 节点的分裂与插入
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    Create a new node and move half the cells over.
    Insert the new value in one of the two nodes.
    Update parent or create a new parent.
    */

    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(old_node); // 获取最大值
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node); // 新节点的父等于老节点，都是需要跟新的父母链接
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node); // 新节点复制所有老节点的sibling
    *leaf_node_next_leaf(old_node) = new_page_num; // old节点的sibling为新节点

    /*
    All existing keys plus new key should should be divided
    evenly between old (left) and new (right) nodes.
    Starting from the right, move each key to correct position.
    */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        // 非常重要的地方，之前设置的是uint32_t，是错误的
        void* destination_node;
        // 确定节点的在左孩子还是右孩子
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {//大于左边计数
            destination_node = new_node; // new_node代表右孩子，相对来说大的
        } else {
            destination_node = old_node; // old_node代表左孩子，相对来说小的
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);

        if (i == cursor->cell_num) { // 分离的目标节点
            serialize_row(value,
                          leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        } else if (i > cursor->cell_num) { // 因为已经分离出去，所以大于的要缩进
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) { //如果是root，则需要重新构建root
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(old_node);
        void* parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }

}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    // 返回节点，没有的话新建
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full 实现Btree节点分开
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        // 从后向前复制
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            // memcpy (str1, str2, size)，将str2的size字节复制到str1，向后移动一位
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
        // 若为空，直接进行下一次循环，
        // 若不为空，代表内容需要存储进入磁盘中，需要刷新进入磁盘中
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    // 关闭错误
    int result = close(pager->file_descripter);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    // 释放所有的内存
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

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}
//new print function
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch (get_node_type(node)) {
        case (NODE_LEAF):
            num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            printf("- leaf (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                printf("- %d\n", *leaf_node_key(node, i));
            }
            break;
        case (NODE_INTERNAL):
            num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            printf("- internal (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                child = *internal_node_child(node, i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                printf("- key %d\n", *internal_node_key(node, i));
            }
            child = *internal_node_right_child(node);
            print_tree(pager, child, indentation_level + 1);
            break;
    }
}

MetaCommandResult do_meta_command(InputBuffer* inputBuffer, Table* table) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else if (strcmp(inputBuffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
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
// 现在，每当我们想要将游标前进到叶节点的末尾时，我们都可以检查叶节点是否有兄弟节点。
// 如果是这样，请跳转到它。 否则，我们就在桌子的最后。
// 仅仅前进一步，如果到下一页，直接就是下一页的开头
void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        /* Advance to next leaf node*/
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
            /* This was rightmost leaf*/
            cursor->end_of_table = true;
        }
        else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

// 执行插入，任务的作用点是在leaf node上！！！
ExecuteResult execute_insert(Statement* statement, Table* table) {
    Row* row_to_insert = &(statement->row_to_insert); // id user_name email
    // Cursor* cursor = table_end(table); 不再需要插入到最后一页
    // table 是表，现有位置，然后根据table存储的num_rows计算应该插入到那一页
    uint32_t key_to_insert = row_to_insert->id; // 根据ID确定插入的位置
    Cursor* cursor = table_find(table, key_to_insert);

    void* node = get_page(table->pager, cursor->page_num);
    // 返回本节点的page位置，转换为node指针。
    uint32_t num_cells = *leaf_node_num_cells(node); // 本节点内，cell数目

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    free(cursor);

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
    // char* filename = "mydb.db"; //debug
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

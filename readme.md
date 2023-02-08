[原教程地址](https://cstack.github.io/db_tutorial/)

# 20230202

## Simple_Database P1

REPL(read eval print loop)**读取-求值-输出”循环**

```c
size_t; //为了增强程序的可移植性，便有了size_t，它是为了方便系统之间的移植而定义的，不同的系统上，定义size_t可能不一样。
ssize_t; //而ssize_t这个数据类型用来表示可以被执行读写操作的数据块的大小.它和size_t类似,但必需是signed.意即：它表示的是signed size_t类型的。
```

### getline

```c
#include <stdio.h>
ssize_t getline(char **restrict lineptr, size_t *restrict n, FILE *restrict stream);
/*
getline() reads an entire line from stream, storing the address of the buffer containing the text into *lineptr. The buffer is null-terminated and includes the newline character, if one was found.

lineptr : a pointer to the variable we use to point to the buffer containing the read line. If it set to NULL it is mallocatted by getline and should thus be freed by the user, even if the command fails.

n : a pointer to the variable we use to save the size of allocated buffer.

stream : the input stream to read from. We’ll be reading from standard input.

return value : the number of bytes read, which may be less than the size of the buffer.
*/
```

**Example**

```c
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
```

# 20230203

## Simple_Database P2

```c
typedef enum{
  //typedef 将枚举类型定义成别名，利用该别名进行变量声明
}name;

strncmp
//int strncmp(const char *str1, const char *str2, size_t n) 把 str1 和 str2 进行比较，最多比较前 n 个字节。
```

### enum

重要的一点，因为枚举类可以定义枚举类型的大小，**第一个枚举成员的默认值为整型的 0，后续枚举成员的值在前一个成员上加 1。我们在这个实例中把第一个枚举成员的值定义为 1，第二个就为 2，以此类推。可以在定义枚举类型时改变枚举元素的值：**

```
enum season {spring, summer=3, autumn, winter};
```

***没有指定值的枚举元素，其值为前一元素加 1。也就说 spring 的值为 0，summer 的值为 3，autumn 的值为 4，winter 的值为 5***

#### 定义方式

**1.先定义枚举类型，再定义枚举变量**

```c
enum DAY
{
      MON=1, TUE, WED, THU, FRI, SAT, SUN
};
enum DAY day;
```

**2.定义枚举类型的同时定义枚举变量**

```
enum DAY
{
      MON=1, TUE, WED, THU, FRI, SAT, SUN
} day;
```

**3.省略枚举名称，直接定义枚举变量**

```
enum
{
      MON=1, TUE, WED, THU, FRI, SAT, SUN
} day;
```

在C 语言中，枚举类型是被当做 int 或者 unsigned int 类型来处理的，所以按照 C 语言规范是没有办法遍历枚举类型的。

不过在一些特殊的情况下，枚举类型必须连续是可以实现有条件的遍历。

以下实例使用 for 来遍历枚举的元素：

```c
#include <stdio.h>
 
enum DAY
{
      MON=1, TUE, WED, THU, FRI, SAT, SUN
} day;
int main()
{
    // 遍历枚举元素
    for (day = MON; day <= SUN; day++) {
        printf("枚举元素：%d \n", day);
    }
}
```

以下枚举类型不连续，

```c
enum
{
    ENUM_0,
    ENUM_10 = 10,
    ENUM_11
}day;
int main()
{
    // 遍历枚举元素
    for (day = ENUM_0; day <= ENUM_11; day++) {
        printf("枚举元素：%d \n", day);
    }
}
//结果
/*
枚举元素0
枚举元素1
...
枚举元素10
枚举元素11
*/
```

# 20230204

## Simple_Database P3

```c
int sscanf(const char *str, const char *format, ...); //从字符串读取格式化输入。
void *memcpy(void *str1, const void *str2, size_t n); //从存储区 str2 复制 n 个字节到存储区 str1。
```

存储数据为

| column   | type         |
| :------- | :----------- |
| id       | integer      |
| username | varchar(32)  |
| email    | varchar(255) |

其中这些东西称为一行数据`Row`，`statement`代表我们需要进行执行的数据和声明，`Table`用来代表指向`page`的指针数组，同时记录数量

```c
typedef struct {
    uint32_t  id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
}Row;

typedef struct {
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;
```

我们需要将该数据复制到表示表的某个数据结构中。 SQLite 使用 B 树进行快速查找、插入和删除。 我们将从更简单的事情开始。 像 B 树一样，它将行分组为页面，但不是将这些页面排列为树，而是将它们排列为数组。

所以我们需要在内存中开辟一片空间存储`pages`，`page`存储`row`，`row`被序列化为每个`page`的紧凑表示，同时保证一个指针数组存储page。

将我们的页面大小设置为 4 KB，因为它与大多数计算机体系结构的虚拟内存系统中使用的页面大小相同。 这意味着我们数据库中的一页对应于操作系统使用的一页。 操作系统会将页面作为整体单元移入和移出内存，而不是将它们分解。

行不应跨越页面边界。 由于页面在内存中可能不会彼此相邻存在，因此这种假设使得读取/写入行更容易。

主程序执行命令逻辑

```c
int main(int argc, char* argv[]) {
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        // .statement代表.命令，没有就是插入数据等其他任务
      	// 首先判定是否存在.命令，当前仅有一个.exit，有的话执行命令
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
      	// 之后进行数据整理，前期准备，插入数据准备好。statement记录命令类型与插入的数据
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
        }
      	// 执行插入数据。
        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }
    return 0;
}
```

# 20230205

## Simple_Database P4

**Rspec的使用**

在C中，`string`要以空字符结尾。但是在`insert`过程中，没有判定插入字符大小，所以在`prepare_statement`中将`scanf`函数替换，使用`strtok`，根据结尾进行分割

```c
char * strtok ( char * str, const char * delimiters );
```

**对该函数的一系列调用将 str 拆分为标记，这些标记是由作为定界符一部分的任何字符分隔的连续字符序列。找到最后一个标记的点由函数内部保存，以供下次调用使用（不需要特定的库实现来避免数据竞争）。也就是会出现strtok(NULL, " ")的原因！！！**

```c
PrepareResult prepare_Insert(InputBuffer* inputBuffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(inputBuffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    
    if (id_string == NULL || username == NULL || email == NULL) 
        return PREPARE_SYNTAX_ERROR;
    
    int id = atoi(id_string);
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

PrepareResult prepare_statement(InputBuffer* inputBuffer, Statement* statement) {
    // 把 str1 和 str2 进行比较，最多比较前 n 个字节。
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(
                inputBuffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
                statement->row_to_insert.username, statement->row_to_insert.email);
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;// 参数数量不够
        }
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}
```

# 20230205

## Simple_Database P5

因为数据是存储在内存当中的，我们需要将数据持久化表示。**we can simply write those blocks of memory to a file, and read them back into memory the next time the program starts up.**

新建结构`Pager`，同时将Table中的页抽象进入`Pager`结构中。

```c
typedef struct {
    int file_descripter;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
}Pager;

// 指向页的数组指针
typedef struct {
    uint32_t num_rows;
    Pager* pager;
} Table;
```

`lseek` [用法](https://blog.csdn.net/songyang516/article/details/6779950)所有打开的文件都有一个当前文件偏移量（current file offset），以下简称为 cfo。cfo 通常是一个非负整数，用于表明文件开始处到文件当前位置的字节数。读写操作通常开始于 cfo，并且使 cfo 增大，增量为读写的字节数。文件被打开时，cfo 会被初始化为 0，除非使用了O_APPEND 。

The `get_page()` method has the logic for handling a cache miss. We assume pages are saved one after the other in the database file: Page 0 at offset 0, page 1 at offset 4096, page 2 at offset 8192, etc. If the requested page lies outside the bounds of the file, we know it should be blank, so we just allocate some memory and return it. The page will be added to the file when we flush the cache to disk later.

```c
// 新建page然后在内存中开辟空间，之后刷新到磁盘中
void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    // Cache miss. Allocate memory and load from file
    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // we might save a partial page at the end of the file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        // 在中间插入
        if (page_num <= num_pages) {
            lseek(pager->file_descripter, page_num * PAGE_SIZE, SEEK_SET); //设置文件偏移量
            ssize_t bytes_read = read(pager->file_descripter, page, PAGE_SIZE); //将文件转移进入page中
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}
```

For now, we’ll wait to flush the cache to disk until the user closes the connection to the database. When the user exits, we’ll call a new method called `db_close()`,将Cache刷新进入磁盘中，关闭数据库 file，然后释放内存

### 大端小端

比如向内存地址为**0X1000**的地址写入**0X12345678**这个四字节16进制数，

**大端模式，即低地址存放低字节数**

| 数据     | 0X12   | 0X34   | 0X56   | 0X78   |
| -------- | ------ | ------ | ------ | ------ |
| 内存地址 | 0X1000 | 0X1001 | 0X1002 | 0X1003 |

内存递增----->

**小端模式，即低地址存放高字节数**

| 数据     | 0X78   | 0X56   | 0X34   | 0X12   |
| -------- | ------ | ------ | ------ | ------ |
| 内存地址 | 0X1000 | 0X1001 | 0X1002 | 0X1003 |

内存递增----->

小端模式 ：强制转换数据不需要调整字节内容，1、2、4字节的存储方式一样。

电脑一般使用小端方式存储

# 20230205

## Simple_Database P6

新加入`Cursor` 类，表示table中的某一个位置，在表的开头创建游标，在表格末尾创建游标，访问光标指向的行，将光标前进到下一行。删除游标指向的行，修改游标指向的行，在表中搜索给定 ID，并创建一个指向具有该 ID 的行的游标。

```c
typedef struct {
    Table* table;
    uint32_t row_num;
    bool end_of_table; // Indicates a position one past the last element
}Cursor;
//the row is the way we can identify a location.
```

Finally we can change our “virtual machine” methods to use the cursor abstraction. When inserting a row, we open a cursor at the end of table, write to that cursor location, then close the cursor.

好了，就是这样！ 正如我所说，这是一个较短的重构，应该有助于我们将表数据结构重写为 B 树。 execute_select() 和 execute_insert() 可以完全通过游标与表交互，而无需对表的存储方式做任何假设。

# 20230205

## Simple_Database P7

[B-Tree1](https://cstack.github.io/db_tutorial/parts/part7.html)

[B-Tree2](https://segmentfault.com/a/1190000020416577)

### Tree 内部节点的介绍！非常重要

**Nodes with children are called “internal” nodes. Internal nodes and leaf nodes are structured differently**

| For an order-m tree… | Internal Node                 | Leaf Node           |
| :------------------- | :---------------------------- | :------------------ |
| Stores               | keys and pointers to children | keys and values     |
| Number of keys       | up to m-1                     | as many as will fit |
| Number of pointers   | number of keys + 1            | none                |
| Number of values     | none                          | number of keys      |
| Key purpose          | used for routing              | paired with value   |
| Stores values?       | No                            | Yes                 |

- up to 3 children per internal node
- up to 2 keys per internal node
- at least 2 children per internal node
- at least 1 key per internal node

树开始的状态

![](https://cstack.github.io/db_tutorial/assets/images/btree1.png)

**If we insert a couple key/value pairs, they are stored in the leaf node in sorted order.**

![](https://cstack.github.io/db_tutorial/assets/images/btree2.png)

**Let’s say that the capacity of a leaf node is two key/value pairs. When we insert another, we have to split the leaf node and put half the pairs in each node. Both nodes become children of a new internal node which will now be the root node.**

![](https://cstack.github.io/db_tutorial/assets/images/btree3.png)

**!!!The internal node has 1 key and 2 pointers to child nodes. If we want to look up a key that is less than or equal to 5, we look in the left child. If we want to look up a key greater than 5, we look in the right child.**

**Now let’s insert the key “2”. First we look up which leaf node it would be in if it was present, and we arrive at the left leaf node. The node is full, so we split the leaf node and create a new entry in the parent node.**

![](https://cstack.github.io/db_tutorial/assets/images/btree4.png)

**Let’s keep adding keys. 18 and 21. We get to the point where we have to split again, but there’s no room in the parent node for another key/pointer pair.**

![](https://cstack.github.io/db_tutorial/assets/images/btree5.png)

**The solution is to split the root node into two internal nodes, then create new root node to be their parent.**

![](https://cstack.github.io/db_tutorial/assets/images/btree6.png)

**The depth of the tree only increases when we split the root node. Every leaf node has the same depth and close to the same number of key/value pairs, so the tree remains balanced and quick to search.**

**When we implement this data structure, each node will correspond to one page. The root node will exist in page 0. Child pointers will simply be the page number that contains the child node.**



# 20230205

## Simple_Database P8

`internal node`将通过存储存储子节点的页码来指向它们的子节点。 btree 向分页器询问特定的页码，并返回指向页面缓存的指针。 页面按页码顺序一个接一个地存储在数据库文件中。

We’re changing the format of our table from an unsorted array of rows to a B-Tree.

如果我们将表存储为数组，但按 id 对行进行排序，我们可以使用二进制搜索来查找特定的 id。 但是，插入会很慢，因为我们必须移动很多行来腾出空间。

相反，我们将使用树结构。 树中的每个节点都可以包含可变数量的行，因此我们必须在每个节点中存储一些信息以跟踪它包含多少行。 另外还有所有不存储任何行的内部节点的存储开销。 换取更大的数据库文件，我们得到了快速的插入、删除和查找。

|               | Unsorted Array of rows | Sorted Array of rows | Tree of nodes                    |
| :------------ | :--------------------- | :------------------- | -------------------------------- |
| Pages contain | only data              | only data            | metadata, primary keys, and data |
| Rows per page | more                   | more                 | fewer                            |
| Insertion     | O(1)                   | O(n)                 | O(log(n))                        |
| Deletion      | O(n)                   | O(n)                 | O(log(n))                        |
| Lookup by id  | O(n)                   | O(log(n))            | O(log(n))                        |

### 结构梳理

`node` -> (`NODE_INTERNAL`,  `NODE_LEAF`)，每一个`node`对应一个`page`，`node` = `page`

`NODER_INTERNAL` -> `child_node(page number)` -> `child_node(page number)`

```c
// 一个node
typedef struct {
    int file_descripter;
    uint32_t file_length; // 文件长度
    uint32_t num_pages; // 每一个节点对应的页书
    void* pages[TABLE_MAX_PAGES]; // 指向page的指针
} Pager;
// 一个表table
typedef struct {
    uint32_t root_page_num; // root节点所在位置
    Pager* pager; // node指针节点。
} Table;

typedef struct {
    Table* table;
		uint32_t row_num;
	 	int32_t page_num;
	 	uint32_t cell_num;
   bool end_of_table;  // Indicates a position one past the last element
 } Cursor;

//共同拥有的header，这个Node本身并不存在，是根据情况梳理的
struct Common Node Header{
	uint32_t NODE_TYPE; // 类型
  uint32_t IS_ROOT; // 是否为root
  uint32_t PARENT_POINTER; // parent 指针
}; 
//叶节点特有的头部
struct Leaf Node Header{
  uint32_t LEAF_NODE_NUM_CELLS; // 有多少键值对
};
//叶节点身体
struct Leaf Body{
  key:value;
};

```

除了这些常见的头字段，叶节点还需要存储它们包含多少“单元格”。 单元格是key/value。

叶节点的主体是一个单元格数组。 每个单元格都是一个键，后跟一个值（序列化行）。

![](https://cstack.github.io/db_tutorial/assets/images/leaf-node-format.png)

### 实现过程

**每个节点都将只占用一页，即使它未满。 这意味着我们的分页器不再需要支持读/写部分页面。**

现在我们的数据库中存储页数而不是行数更有意义。 页数应该与 pager 对象相关联，而不是表，因为它是数据库使用的页数，而不是特定表。 btree 由其根节点页码标识，因此表对象需要对其进行跟踪。

`cursor`代表表中的一个位置。 当我们的表是一个简单的行数组时，我们可以只给定行号来访问一行。 现在它是一棵树，我们通过节点的页码和该节点内的单元格编号来识别位置。

#### insert data

在本文中，我们只打算实现足以获得单节点树的功能。 回想一下上一篇文章，一棵树从一个空叶节点开始。

首先是空树 empty btree

![](https://cstack.github.io/db_tutorial/assets/images/btree1.png)

一个node的btree



![](https://cstack.github.io/db_tutorial/assets/images/btree2.png)



当我们第一次打开数据库时，数据库文件会是空的，所以我们将第0页初始化为一个空的叶子节点（根节点）：

```c
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
```

我们还没有实现拆分，所以如果节点已满，我们会出错。 接下来，我们将单元格向右移动一个空格，为新单元格腾出空间。 然后我们将新的键/值写入空白区域。

#### get-page重点函数关注问题

```c
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
```

# 20230205

## Simple_Database P9

`key` 存储是无序的，加上检测并拒绝重复键。所以在`execute_insert()` 中，之前是选择在table尾部添加，现在选择在合适的位置添加，若`key` 已经存在，返回错误。

修改函数`execute_insert`，增加函数`table_find()` 和`leaf_node_find` 和  `get_node_type` 完成结果是使用。

# 20230205

## Simple_Database P10- Splitting a Leaf Node

我们的 B-Tree 不像只有一个节点的树。 为了解决这个问题，我们需要一些代码来将叶节点一分为二。 之后，我们需要创建一个内部节点作为两个叶节点的父节点。

one-node btree

![](https://cstack.github.io/db_tutorial/assets/images/btree2.png)

最后改变成

![](https://cstack.github.io/db_tutorial/assets/images/btree3.png)





### Splitting Algorithm

在之前，只是添加判断节点是否存在，同时如果节点超出MAX限制后，我们之间进行退出操作，并没有将`leafNode` 节点进行分裂。为了解决这个问题，我们需要一些代码来将叶节点一分为二。 之后，我们需要创建一个内部节点作为两个叶节点的父节点。

> If there is no space on the leaf node, we would split the existing entries residing there and the new one (being inserted) into two equal halves: lower and upper halves. (Keys on the upper half are strictly greater than those on the lower half.) We allocate a new leaf node, and move the upper half into the new node.

```c
// 节点的分裂与插入
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
     * Create a new node and move half the cells over.
     * Insert the new value in one of the two nodes.
     * Update parent or create a new parent.
     * */
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    /*
     * All existing keys plus new key should be divided
     * evenly between old (left) and new (right) nodes.
     * Starting from the right, move each key to correct position.
     */
    for (uint32_t i = LEAF_NODE_MAX_CELLS; i >= 0; --i) {
        void* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) { //大于左边计数
            destination_node = new_node;
        }
        else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);
        if (i == cursor->cell_num) { // 分离的目标节点
            serialize_row(value, destination);
        }
        else if (i > cursor->cell_num) {// 因为已经分离出去，所以大于的要锁进
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        }
        else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }
    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    /* update parent*/
    if (is_node_root(old_node)) {
        return creat_new_root(cursor->table, new_page_num);
    }
    else {
        printf("Need to implement update parent after split\n");
        exit(EXIT_FAILURE);
    }
}
```

### Creating a New Root

> If there is no space on the leaf node, we would split the existing entries residing there and the new one (being inserted) into two equal halves: lower and upper halves. (Keys on the upper half are strictly greater than those on the lower half.) We allocate a new leaf node, and move the upper half into the new node.

此时，我们已经分配了右孩子并将上半部分移入其中。 我们的函数将右孩子作为输入并分配一个新页面来存储左孩子。

```c
void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
     * Handle splitting the root.
     * Old root copied to new page, becomes left child.
     * Address of right child passed in.
     * Re-initialize root page to contain the new root node.
     * New root node points to two children.
     	*	处理分裂根。
      * 旧根复制到新页面，成为左孩子。
      * 右孩子的地址传入。
      * 重新初始化根页面以包含新的根节点。
      * 新的根节点指向两个孩子。
     */
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
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
}
```

## Internal Node Format

现在我们终于创建了一个内部节点，我们必须定义它的布局。 它以公共标题开头，然后是它包含的键数，**然后是其最右边子项(rightmost)的页码**。 内部节点的子指针总是比它们的键多一个。 该额外的子指针存储在标头中。

Based on these constants, here’s how the layout of an internal node will look:

![](https://cstack.github.io/db_tutorial/assets/images/internal-node-format.png)

**For an internal node, the maximum key is always its right key. For a leaf node, it’s the key at the maximum index**

# 20230206

## Simple_Database P11

This function will perform binary search to find the child that should contain the given key. **Remember that the key to the right of each child pointer is the maximum key contained by that child.** `internal`中`right_child_pointer`表示最大值key存储的的孩子指针。

![](https://cstack.github.io/db_tutorial/assets/images/btree6.png)

# 20230206

## Simple_Database P12

并添加一个搜索键 0（最小可能键）的新实现。 即使 key 0 在表中不存在，该方法也会返回最低 id 的位置（最左边叶节点的开始）。

要扫描整个表，我们需要在到达第一个叶节点的末尾后跳转到第二个叶节点。 为此，我们将在叶节点标头中保存一个名为“next_leaf”的新字段，它将在右侧保存叶的兄弟节点的页码。 最右边的叶节点的 next_leaf 值为 0，表示没有兄弟节点（页 0 无论如何都为表的根节点保留）。

**每当我们拆分叶节点时，更新兄弟指针。 旧叶子的兄弟成为新叶子，新叶子的兄弟成为旧叶子的兄弟。**

现在，每当我们想要将游标前进到叶节点的末尾时，我们都可以检查叶节点是否有兄弟节点。 如果是这样，请跳转到它。 否则，我们就在桌子的最后。

# 20230206

## Simple_Database P13

![](https://cstack.github.io/db_tutorial/assets/images/updating-internal-node.png)

In this example, we add the key “3” to the tree. That causes the left leaf node to split. After the split we fix up the tree by doing the following:

1. Update the first key in the parent to be the maximum key in the left child (“3”)
2. Add a new child pointer / key pair after the updated key
   - The new pointer points to the new child node
   - The new key is the maximum key in the new child node (“5”)

So first things first, replace our stub code with two new function calls: `update_internal_node_key()` for step 1 and `internal_node_insert()` for step 2

### 错误记录

```c
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    ....
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    ....
    // 错误代码
    //for (uint32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
```

因为赋值后，`uint32_t`的`i`，因为是无符号数，所以在变为0的时候，它不会停止，会继续向下减。例如：

```c
int flag = 5;
    for (uint32_t i = 1; i >= 0; --i, -- flag) {
        printf("%d\n", i);
        if (!flag)
            break;
    }
/*运行结果
1
0
-1 i = 4294967295
-2 i = 4294967294
-3 i = 4294967293
-4 i = 4294967292
*/
```

|          |              |          | Bits | Bytes | Minimum value  | Maximum value |
| -------- | ------------ | -------- | ---- | ----- | -------------- | ------------- |
| int32_t  | int          | signed   | 32   | 4     | -2,147,483,648 | 2,147,483,647 |
| uint32_t | unsigned int | Unsigned | 32   | 4     | 0              | 4,294,967,295 |

# 20230206

## Simple_Database P14

这里需要添加函数`interanl_split`，因为在leaf分裂后，Leaf Page满、internal Page满时，拆分Leaf Page，小于中间节点的记录放左边，大于中间节点的记录放右边；拆分Index Page，原理同上，此时树的高度+1。

情况分类，因为设定我们 `internal_node` 的key最大是2个，最多3个孩子。

情况1，10要进入internal

```
  4        8                         4        8     10
1,4  5,6,8   9,10,11,12           1,4    5,6,8   9,10   11,12
```

情况2， 

```
  4        8                         4        6     8
1,4  5,6,7,8   10,11,12           1,4    5,6,   7,8   10,11,12
```

情况3，2进入internal

```
       4         8                         2     4       8
1,2,3,4  5,6,8      9,11,12           1,2    3,4   5,6,8   9,11,12
```

所以给予我们思考的就是如何将最后的结果部分，进行拆分。这里肯定提升的节点为中间节点。

**这里进行了偷懒，我先执行了`internal_node_insert` 即使超出了最大限度，我们也要添加，因为我们设置的为3，而页很大。这里其实是应该先判定孩子链接在哪里，最大值在哪里然后进行插入。**

#ifndef NVMKV_ERT_NODE_INT_H
#define NVMKV_ERT_NODE_INT_H

#include <cstdint>
#include <sys/time.h>
#include <vector>
#include <map>
#include <math.h>
#include <cstdint>
#include "../fastalloc/fastalloc.h"

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

// 计算给定key在指定深度depth处的段编号（segment number），取key的 低key_len位中 最高的 depth 位
#define GET_SEG_NUM(key, key_len, depth)  ((key>>(key_len-depth))&(((uint64_t)1<<depth)-1))
// 计算bucket编号，取key的最低 bucket_mask_len 位
#define GET_BUCKET_NUM(key, bucket_mask_len) ((key)&(((uint64_t)1<<bucket_mask_len)-1))
// 计算对应 segment 位置
// 节点的起始位置+整个节点偏移，即定位到节点末尾，然后加上 dir_index 个指针的便宜，即定位到第dir_index（从0开始）个seg位置处。
#define GET_SEG_POS(currentNode,dir_index) (((uint64_t)(currentNode) + sizeof(ERTIntNode) + dir_index*sizeof(ERTIntNode*)))
// 获取子key，从key的 start 位开始，截取 length 长度。
// key>>(64 - start - length) 右移，保留 start+length 之前的位数，然后取低 length 位。
#define GET_SUBKEY(key, start, length) ( (key>>(64 - start - length) & (((uint64_t)1<<length)-1)))
// 高八位flag全置为0，其余位不变
#define REMOVE_NODE_FLAG(key) (key & (((uint64_t)1<<56)-1) )
// 只设置第56位，该标识表示value中存储的是kv值（可以是一个，也可以是多个），而不是指向下一个节点。
#define PUT_KEY_VALUE_FLAG(key) (key | ((uint64_t)1<<56))
// 节点的前8位为flag位
#define GET_NODE_FLAG(key) (key>>56)
// 全局depth默认为0
#define ERT_INIT_GLOBAL_DEPTH 0
// bucket内默认4个key
#define ERT_BUCKET_SIZE 4
// key的最低8位代表对应bucket，所以最大bucket数量位2^8
#define ERT_BUCKET_MASK_LEN 8
#define ERT_MAX_BUCKET_NUM (1<<ERT_BUCKET_MASK_LEN)

#define SIZE_OF_CHAR 8
// 每个节点 span 32位（即节点保存32位前缀？）
#define ERT_NODE_LENGTH 32
#define ERT_NODE_PREFIX_MAX_BYTES 6
#define ERT_NODE_PREFIX_MAX_BITS 48
#define ERT_KEY_LENGTH 64

class ERTIntKeyValue {
public:
    uint64_t key = 0;// indeed only need uint8 or uint16
    uint64_t value = 0;

    void operator =(ERTIntKeyValue a){
        this->key = a.key;
        this->value = a.value;
    };
};

ERTIntKeyValue *NewERTIntKeyValue(uint64_t key, uint64_t value);

class ERTIntBucketKeyValue{
public:
    uint64_t subkey = 0;
    uint64_t value = 0;
};

class ERTIntBucket {
public:

    ERTIntBucketKeyValue counter[ERT_BUCKET_SIZE];

    uint64_t get(uint64_t key, bool& keyValueFlag);

    int findPlace(uint64_t _key, uint64_t _key_len, uint64_t _depth);
};

class ERTIntSegment {
public:
    // local depth，决定bucket idx
    uint64_t depth = 0;
    ERTIntBucket *bucket;
//    ERTIntBucket bucket[ERT_MAX_BUCKET_NUM];

    ERTIntSegment();

    ~ERTIntSegment();

    void init(uint64_t _depth);
};

ERTIntSegment *NewERTIntSegment(uint64_t _depth = 0);

class ERTIntHeader{
public:
    // 前缀长度
    unsigned char len = 7;
    // depth 是基数树的深度
    unsigned char depth;
    // 存储前缀的数组
    unsigned char array[6];

    void init(ERTIntHeader* oldHeader, unsigned char length, unsigned char depth);

    int computePrefix(uint64_t key, int pos);

    void assign(uint64_t key, int startPos);

    void assign(unsigned char* key, unsigned char assignedLength = ERT_NODE_PREFIX_MAX_BYTES);
};

class ERTIntNode {
public:
    ERTIntHeader header;
    // global_depth决定hash dir idx
    unsigned char global_depth = 0;
    uint32_t dir_size = 1;
    // 存储数据，当key匹配完前面的节点，最后一部分落在这个节点内时，value存储在这个数组里，最多存储header.len个value。
    // 注意：这里如果最终匹配到header.array的长度为 matchedPrefixLen，则value放在这个数组下标为 header.len-matchedPrefixLen 位置。
    // 即：完全匹配的放在idx 0位置，完全不匹配的在idx header.len位置
    // TODO（chen）这里为什么不正向放呢，假如最终匹配到header.array的长度为 m，则value放在这个数组的 m-1 下标位置。
    ERTIntKeyValue* treeNodeValues;
    // used to represent the elements in the treenode prefix, but not in CCEH

    ERTIntNode();

    ~ERTIntNode();

    void init( unsigned char headerDepth = 0, unsigned char global_depth = 0);

    void put(uint64_t subkey, uint64_t value, uint64_t beforeAddress);

    void put(uint64_t subkey, uint64_t value, ERTIntSegment* tmp_seg, ERTIntBucket* tmp_bucket, uint64_t dir_index, uint64_t seg_index, uint64_t beforeAddress);

    void nodePut(int pos, ERTIntKeyValue *kv);

    uint64_t get(uint64_t subkey, bool& keyValueFlag);

};

ERTIntNode *NewERTIntNode(int _key_len, unsigned char headerDepth = 1,
                          unsigned char globalDepth = ERT_INIT_GLOBAL_DEPTH);


#endif //NVMKV_ERT_NODE_INT_H

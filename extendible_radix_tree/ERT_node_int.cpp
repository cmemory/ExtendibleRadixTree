#include "ERT_node_int.h"

// mfence(); 是一个内存屏障指令，确保前面的内存操作 在系统执行 接下来的操作 之前完全完成。避免乱序执行导致的潜在数据一致性问题。
inline void mfence(void) {
    asm volatile("mfence":: :"memory");
}

// 用于将指定范围内的内存从 CPU 缓存刷新到主存或持久性存储中，以确保在发生崩溃或断电时数据不会丢失。
inline void clflush(char *data, size_t len) {
    volatile char *ptr = (char *) ((unsigned long) data & (~(CACHELINESIZE - 1)));
    mfence();
    for (; ptr < data + len; ptr += CACHELINESIZE) {
        asm volatile("clflush %0" : "+m" (*(volatile char *) ptr));
    }
    mfence();
}

ERTIntKeyValue *NewERTIntKeyValue(uint64_t key, uint64_t value) {
    ERTIntKeyValue *_new_key_value = static_cast<ERTIntKeyValue *>(concurrency_fast_alloc(sizeof(ERTIntKeyValue)));
    _new_key_value->key = key;
    _new_key_value->value = value;
    return _new_key_value;
}


// 从bucket中查询指定key，返回value和flag
uint64_t ERTIntBucket::get(uint64_t key, bool &keyValueFlag) {
    for (int i = 0; i < ERT_BUCKET_SIZE; ++i) {
        // 移除高8位flag后，低56位是key。
        // 判断是否是查询key，如果是查到了，返回value和flag
        if (key == REMOVE_NODE_FLAG(counter[i].subkey)) {
            keyValueFlag = GET_NODE_FLAG(counter[i].subkey);
            return counter[i].value;
        }
    }
    return 0;
}

int ERTIntBucket::findPlace(uint64_t _key, uint64_t _key_len, uint64_t _depth) {
    // full: return -1
    // exists or not full: return index or empty counter
    int res = -1;
    for (int i = 0; i < ERT_BUCKET_SIZE; ++i) {
        uint64_t removedFlagKey = REMOVE_NODE_FLAG(counter[i].subkey);
        if (_key == removedFlagKey) {
            // 存在对应key，返回key的idx
            return i;
        } else if ((res == -1) && removedFlagKey == 0 && counter[i].value == 0) {
            // 空位置，先赋值给res。还要往后找，万一key存在呢。
            res = i;
        } else if ((res == -1) &&
                   (GET_SEG_NUM(_key, _key_len, _depth) !=
                    GET_SEG_NUM(removedFlagKey, _key_len, _depth))) { // todo: wrong logic
            // 这里取出segment不一致，说明 removedFlagKey 已经已经迁移走了，可以覆盖写。
            res = i;
        }
    }
    return res;
}

ERTIntSegment::ERTIntSegment() {
    depth = 0;
    bucket = static_cast<ERTIntBucket *>(concurrency_fast_alloc(sizeof(ERTIntBucket) * ERT_MAX_BUCKET_NUM));
}

ERTIntSegment::~ERTIntSegment() {}

void ERTIntSegment::init(uint64_t _depth) {
    depth = _depth;
    bucket = static_cast<ERTIntBucket *>(concurrency_fast_alloc(sizeof(ERTIntBucket) * ERT_MAX_BUCKET_NUM));
}


ERTIntSegment *NewERTIntSegment(uint64_t _depth) {
    // 申请segment结构
    ERTIntSegment *_new_ht_segment = static_cast<ERTIntSegment *>(concurrency_fast_alloc(sizeof(ERTIntSegment)));
    // segment中存数据的bucket数组结构
    _new_ht_segment->init(_depth);
    return _new_ht_segment;
}

// 使用老的header和新的depth构建新的header，扩容的时候这样操作
void ERTIntHeader::init(ERTIntHeader *oldHeader, unsigned char length, unsigned char depth) {
    assign(oldHeader->array, length);
    this->depth = depth;
    this->len = length;
}

// 计算subkey和header的array公共前缀，即key在当前层的前缀
int ERTIntHeader::computePrefix(uint64_t key, int startPos) {
    if (this->len == 0) {
        return 0;
    }
    // 提取subkey，并扩展到64位，不够的末尾补0
    uint64_t subkey = GET_SUBKEY(key, startPos, (this->len * SIZE_OF_CHAR));
    subkey <<= (64 - this->len * SIZE_OF_CHAR);
    int res = 0;
    for (int i = 0; i < this->len; i++) {
        // 最高位挨个取array比较，不匹配就跳出？
        // TODO（chen）感觉应该是subkey逐字节与array比较，找公共前缀吧？所以循环内最后需要subkey<<8？
        if ((subkey >> 56) != ((uint64_t) array[i])) {
            break;
        }
        res++;
        subkey <<= 8;
    }
    return res;
}

void ERTIntHeader::assign(uint64_t key, int startPos) {
    // 获取子key
    uint64_t subkey = GET_SUBKEY(key, startPos, (this->len * SIZE_OF_CHAR));
    // array默认6字节，这里扩展到6字节，不够的低字节补0
    subkey <<= (ERT_NODE_PREFIX_MAX_BITS - this->len * SIZE_OF_CHAR);
    // 子key写入到array中，array中0-5，依次对应subkey的0-5字节
    for (int i = ERT_NODE_PREFIX_MAX_BYTES - 1; i >= 0; i--) {
        array[i] = (char) subkey & (((uint64_t) 1 << 8) - 1);
        subkey >>= 8;
    }
}

void ERTIntHeader::assign(unsigned char *key, unsigned char assignedLength) {
    for (int i = 0; i < assignedLength; i++) {
        array[i] = key[i];
    }
}

ERTIntNode::ERTIntNode() {
    global_depth = 0;
    dir_size = pow(2, global_depth);
    // 构造函数空间都没分配，这里直接定位到指针位置构造segment赋值？
    for (int i = 0; i < dir_size; ++i) {
        *(ERTIntSegment **) GET_SEG_POS(this, i) = NewERTIntSegment();
    }
}

ERTIntNode::~ERTIntNode() {}

// 初始化节点，使用 NewERTIntNode 创建节点时先分配空间，再调用这个来初始化。
void ERTIntNode::init(unsigned char headerDepth, unsigned char global_depth) {
    this->global_depth = global_depth;
    this->dir_size = pow(2, global_depth);
    header.depth = headerDepth;
    // 这个节点存储的数据应该是与header.len一样，应该是前缀从只匹配到一个字符到完全都匹配。
    treeNodeValues = static_cast<ERTIntKeyValue *>(concurrency_fast_alloc(
        // TODO（chen）这里不应该除以 ERT_NODE_LENGTH，而应该是 SIZE_OF_CHAR，最大应该是6个才对？
            sizeof(ERTIntKeyValue) * (1 + ERT_NODE_PREFIX_MAX_BITS / ERT_NODE_LENGTH)));
    for (int i = 0; i < this->dir_size; ++i) {
        *(ERTIntSegment **) GET_SEG_POS(this, i) = NewERTIntSegment(global_depth);
    }
}

void ERTIntNode::put(uint64_t subkey, uint64_t value, uint64_t beforeAddress) {
    // subkey 中取 后 ERT_NODE_LENGTH 位，然后取高 global_depth 位，即为segment索引
    uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, global_depth);
    // 根据segment索引找到对应段
    ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(this, dir_index);
    // 根据 subkey 的低 ERT_BUCKET_MASK_LEN 定位到bucket编号。
    uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
    // 根据bucket编号定位找到对应bucket
    ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
    // 往bucket中写入kv。三种情况：
    // 1、有空位直接插入；2、没有空位，段split，但dir不需要扩容；3、没有空位，段split，同时需要dir扩容。
    put(subkey, value, tmp_seg, tmp_bucket, dir_index, seg_index, beforeAddress);
}

void
ERTIntNode::put(uint64_t subkey, uint64_t value, ERTIntSegment *tmp_seg, ERTIntBucket *tmp_bucket, uint64_t dir_index,
                uint64_t seg_index, uint64_t beforeAddress) {
    // bucket找位置，如果有空位则返回对应idx，没有则返回-1
    int bucket_index = tmp_bucket->findPlace(subkey, ERT_NODE_LENGTH, tmp_seg->depth);
    if (bucket_index == -1) {
        //condition: full
        if (likely(tmp_seg->depth < global_depth)) {
            // 1、bucket满了，当local depth < global depth时，无需扩容dir，segment split即可。
            // local depth+1 构建新的segment
            ERTIntSegment *new_seg = NewERTIntSegment(tmp_seg->depth + 1);
            // stride表示在目前的全局和本地depth下，段（segment）占用的dir数量。
            // 当全局和本地depth一样时，只占用一个dir，否则会占用多个dir。
            int64_t stride = pow(2, global_depth - tmp_seg->depth);
            // 目前的全局和本地depth下，指向同一个segment的起始的dir（即split后左边那个segment的起始dir）。
            int64_t left = dir_index - dir_index % stride;
            // 计算指向同一个segment的中间dir（即split后右边那个segment的起始dir）
            // 计算指向同一个segment dir结束位置（即指向下一个segment的起始dir）
            int64_t mid = left + stride / 2, right = left + stride;

            //migrate previous data to the new bucket
            // 遍历原segment的所有bucket处理数据迁移，理论上迁移一半数据
            for (int i = 0; i < ERT_MAX_BUCKET_NUM; ++i) {
                uint64_t bucket_cnt = 0;
                // 每个bucket内有4个kv遍历迁移
                for (int j = 0; j < ERT_BUCKET_SIZE; ++j) {
                    // key占56位，这里移除高八位标识位。
                    uint64_t tmp_key = REMOVE_NODE_FLAG(tmp_seg->bucket[i].counter[j].subkey);
                    uint64_t tmp_value = tmp_seg->bucket[i].counter[j].value;
                    // 计算key的dir，如果dir在split后的右边那个segment，则进行迁移处理。
                    dir_index = GET_SEG_NUM(tmp_key, ERT_NODE_LENGTH, global_depth);
                    if (dir_index >= mid) {
                        ERTIntSegment *dst_seg = new_seg;
                        // bucket编号由低8位决定，所以跟原来一致不变
                        seg_index = i;
                        ERTIntBucket *dst_bucket = &(dst_seg->bucket[seg_index]);
                        // 找到bucket后，按顺序写入。新的bucket中最多迁移一半的元素。
                        dst_bucket->counter[bucket_cnt].value = tmp_value;
                        dst_bucket->counter[bucket_cnt].subkey = tmp_seg->bucket[i].counter[j].subkey;
                        bucket_cnt++;
                    }
                }
            }
            // 新segment持久化
            clflush((char *) new_seg, sizeof(ERTIntSegment));

            // set dir[mid, right) to the new bucket
            // segment split后，将后半部分的dir都指向新segment
            for (int i = right - 1; i >= mid; --i) {
                *(ERTIntSegment **) GET_SEG_POS(this, i) = new_seg;
            }
            // TODO（chen）这里持久化不是从mid开始么，怎么是 right-1
            clflush((char *) GET_SEG_POS(this, right - 1), sizeof(ERTIntSegment *) * (right - mid));

            // 原segment depth+1，并持久化
            tmp_seg->depth = tmp_seg->depth + 1;
            clflush((char *) &(tmp_seg->depth), sizeof(tmp_seg->depth));
            // segment split完成，重新调用put走流程3插入kv
            this->put(subkey, value, beforeAddress);
            return;
        } else {
            //condition: tmp_bucket->depth == global_depth
            // 2、bucket满了，且local depth == global depth时，需要扩容dir，然后segment split。
            // 首先dir扩容分配新的Node空间，global_depth+1，dir_size翻倍，header拷贝原来的
            ERTIntNode *newNode = static_cast<ERTIntNode *>(concurrency_fast_alloc(
                    sizeof(ERTIntNode) + sizeof(ERTIntNode *) * dir_size * 2));
            newNode->global_depth = global_depth + 1;
            newNode->dir_size = dir_size * 2;
            newNode->header.init(&this->header, this->header.len, this->header.depth);
            //set dir
            // 设置新节点的dir，因为新节点的dir翻倍了，所以新节点的i和i+1都指向老节点的i/2。
            for (int i = 0; i < newNode->dir_size; ++i) {
                *(ERTIntSegment **) GET_SEG_POS(newNode, i) = *(ERTIntSegment **) GET_SEG_POS(this, (i / 2));
            }
            // 持久化新节点
            clflush((char *) newNode, sizeof(ERTIntNode) + sizeof(ERTIntSegment *) * newNode->dir_size);
            // 原来某个位置指向老节点持久化的，这里需要把那个地方设置指向新节点并持久化。
            *(ERTIntNode **) beforeAddress = newNode;
            clflush((char *) beforeAddress, sizeof(ERTIntNode *));
            // 处理完了扩容，调用节点的put函数，后面走处理1中segment split的流程。
            newNode->put(subkey, value, beforeAddress);
            return;
        }
    } else {
        // 3、bucket未满，数据可写入
        // 此时分为key存在和不存在两种情况
        if (unlikely(tmp_bucket->counter[bucket_index].subkey == subkey) && subkey != 0) {
            //key exists
            // 存在key，直接value覆盖并持久化。
            tmp_bucket->counter[bucket_index].value = value;
            clflush((char *) &(tmp_bucket->counter[bucket_index].value), 8);
        } else {
            // there is a place to insert
            // key不存在，先写value持久化后再写key，防止中间失败value失效
            tmp_bucket->counter[bucket_index].value = value;
            mfence();
            tmp_bucket->counter[bucket_index].subkey = PUT_KEY_VALUE_FLAG(subkey);
            // Here we clflush 16bytes rather than two 8 bytes because all counter are set to 0.
            // If crash after key flushed, then the value is 0. When we return the value, we would find that the key is not inserted.
            // 持久化kv
            clflush((char *) &(tmp_bucket->counter[bucket_index].subkey), 16);
        }
    }
    return;
}

// 如果k的最后子key包含在节点的前缀里面，则直接存储到节点的 treeNodeValues 中。
// 这里倒着存的，如果时完全匹配，则存储在第一个。如果只匹配第一个字节，则存储在最后位置。
void ERTIntNode::nodePut(int pos, ERTIntKeyValue *kv) {
    treeNodeValues[header.len - pos] = *kv;
}

// 当前节点中找数据，定位dir->找到对应segment->定位到bucket->bucket内部遍历
uint64_t ERTIntNode::get(uint64_t subkey, bool &keyValueFlag) {
    uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, global_depth);
    ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(this, dir_index);
    uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
    ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
    return tmp_bucket->get(subkey, keyValueFlag);
}


ERTIntNode *NewERTIntNode(int _key_len, unsigned char headerDepth, unsigned char globalDepth) {
    ERTIntNode *_new_node = static_cast<ERTIntNode *>(concurrency_fast_alloc(
            sizeof(ERTIntNode) + sizeof(ERTIntSegment *) * pow(2, globalDepth)));
    _new_node->init(headerDepth, globalDepth);
    return _new_node;
}
#include "ERT_int.h"

/*
 *         begin  len
 * key [______|___________|____________]
 */

// 从内存中的指定位置读取一个32位的无符号整数。pointer是指针，pos是偏移
#define GET_32BITS(pointer, pos) (*((uint32_t *)(pointer+pos)))

#define _32_BITS_OF_BYTES 4


inline void mfence(void) {
    asm volatile("mfence":: :"memory");
}

inline void clflush(char *data, size_t len) {
    volatile char *ptr = (char *) ((unsigned long) data & (~(CACHELINESIZE - 1)));
    mfence();
    for (; ptr < data + len; ptr += CACHELINESIZE) {
        asm volatile("clflush %0" : "+m" (*(volatile char *) ptr));
    }
    mfence();
}

bool isSame(unsigned char *key1, uint64_t key2, int pos, int length) {
    // 从key2的pos位置截取length长度的子key
    uint64_t subkey = GET_SUBKEY(key2, pos, length);
    // 子key低位补0，补全64位
    subkey <<= (64 - length);
    for (int i = 0; i < length / SIZE_OF_CHAR; i++) {
        // 逐字节比较subkey与key1，只要出现不同则返回false
        if ((subkey >> 56) != (uint64_t) key1[i]) {
            return false;
        }
        subkey <<= SIZE_OF_CHAR;
    }
    // 比较完返回true
    return true;
}

void ERTInt::Insert(uint64_t key, uint64_t value, ERTIntNode *_node, int len) {
    // 0-index of current bytes

    // 有传入节点则使用传入的，没有则使用root节点
    ERTIntNode *currentNode = _node;
    if (_node == NULL) {
        currentNode = root;
    }
    // 基数树深度，即树高？
    unsigned char headerDepth = currentNode->header.depth;
    // 默认根节点指向当前节点？
    uint64_t beforeAddress = (uint64_t) &root;

    // 最初传入的len默认是0
    while (len < ERT_KEY_LENGTH / SIZE_OF_CHAR) {
        int matchedPrefixLen;

        // init a number larger than HT_NODE_PREFIX_MAX_LEN to represent there is no value
        // header.len初始设置为7表示节点里面没值，这里超过了最大长度6，需要进行初始相关处理。
        if (currentNode->header.len > ERT_NODE_PREFIX_MAX_BYTES) {
            // 超过了，这里设置为最大size为32位
            int size = ERT_NODE_PREFIX_MAX_BITS / ERT_NODE_LENGTH * ERT_NODE_LENGTH;

            // 第一次插入元素，根据元素来计算得到len
            currentNode->header.len =
                    (ERT_KEY_LENGTH - len) <= size ? (ERT_KEY_LENGTH - len) / SIZE_OF_CHAR : size / SIZE_OF_CHAR;
            // 头array中记录前缀
            currentNode->header.assign(key, len);
            len += size / SIZE_OF_CHAR;
            continue;
        } else {
            // compute this prefix
            // 计算key从第len字节处提取的子key的前缀 与 当前节点存储前缀能匹配多少
            matchedPrefixLen = currentNode->header.computePrefix(key, len * SIZE_OF_CHAR);
        }
        if (len + matchedPrefixLen == (ERT_KEY_LENGTH / SIZE_OF_CHAR)) {
            // 如果前缀能完全匹配key（即之前已有的长度len+当前节点能匹配的长度 等于key的长度），则kv存储在当前节点中。
            // 构建kv并持久化
            ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
            clflush((char *) kv, sizeof(ERTIntKeyValue));
            // 加入到当前节点存储
            currentNode->nodePut(matchedPrefixLen, kv);
            return;
        }
        if (matchedPrefixLen == currentNode->header.len) {
            // if prefix is match
            // move the pos
            // 如果前缀能完全匹配，则需要找下一个节点，len是当前查找到的匹配的长度，后面再处理的key中偏移
            len += currentNode->header.len;

            // compute subkey
            // uint64_t subkey = GET_16BITS(key,pos);
            // 从len开始获取子key
            uint64_t subkey = GET_SUBKEY(key, len * SIZE_OF_CHAR, ERT_NODE_LENGTH);

            // use the subkey to search in a cceh node
            uint64_t next = 0;

            // 在当前节点中，定位到subkey对应的bucket
            uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, currentNode->global_depth);
            ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(currentNode, dir_index);
            uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
            ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
            int i;
            bool keyValueFlag = false;
            uint64_t beforeA;
            for (i = 0; i < ERT_BUCKET_SIZE; ++i) {
                if (subkey == REMOVE_NODE_FLAG(tmp_bucket->counter[i].subkey)) {
                    // bucket中找到了匹配的子key，取得对应value
                    next = tmp_bucket->counter[i].value;
                    keyValueFlag = GET_NODE_FLAG(tmp_bucket->counter[i].subkey);
                    beforeA = (uint64_t) &tmp_bucket->counter[i].value;
                    break;
                }
            }
            len += ERT_NODE_LENGTH / SIZE_OF_CHAR;
            if (len == 8) {
                // len==8 表示达到了key的长度？ 这里应该用 (ERT_KEY_LENGTH / SIZE_OF_CHAR) 替代8吧？
                if (next == 0) {
                    // next==0表示 subkey对应的bucket中没有找到，当前节点插入subkey
                    currentNode->put(subkey, (uint64_t) value, tmp_seg, tmp_bucket, dir_index, seg_index,
                                     beforeAddress);
                    return;
                } else {
                    // 找到了subkey，覆盖写入并持久化
                    tmp_bucket->counter[i].value = value;
                    clflush((char *) &tmp_bucket->counter[i].value, 8);
                    return;
                }
            } else {
                // 没有达到key的长度时，bucket中存储的是kv形式的，有两种情况：
                // 1、如果只有一个key，则 bucket 中对应的value指向这个kv对。
                // 2、如果有多个key，则 bucket 中对应的value指向新的节点。
                // 所以最终bucket中的条目可能指向kv对，也可能指向一个节点。
                // 而对于达到了key长度的叶子节点，bucket中对应的value指向的是查询key的值。
                if (next == 0) {
                    //not exists
                    // bucket里也没找到，subkey无冲突，直接构建kv持久化，并加入到当前节点bucket中。
                    ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
                    clflush((char *) kv, sizeof(ERTIntKeyValue));
                    currentNode->put(subkey, (uint64_t) kv, tmp_seg, tmp_bucket, dir_index, seg_index, beforeAddress);
                    return;
                } else {
                    // 没达到key长度，keyValueFlag 表示bucket中已经有subkey前缀，表示冲突了需要解决
                    // 对应的value是kv形式的，如果key与当前写入的一致，则我们直接更新，否则构建新节点。
                    if (keyValueFlag) {
                        // next is key value pair, which means collides
                        uint64_t prekey = ((ERTIntKeyValue *) next)->key;
                        uint64_t prevalue = ((ERTIntKeyValue *) next)->value;
                        if (unlikely(key == prekey)) {
                            //same key, update the value
                            ((ERTIntKeyValue *) next)->value = value;
                            clflush((char *) &(((ERTIntKeyValue *) next)->value), 8);
                            return;
                        } else {
                            //not same key: needs to create a new node
                            // 插入key不同，则构建新节点，并将原来的kv和新的kv都加入到节点中
                            ERTIntNode *newNode = NewERTIntNode(ERT_NODE_LENGTH, headerDepth + 1);

                            // put pre kv
                            Insert(prekey, prevalue, newNode, len);

                            // put new kv
                            Insert(key, value, newNode, len);

                            clflush((char *) newNode, sizeof(ERTIntNode));

                            // 新节点加入到当前bucket中，存储的是节点不是kv，所以移除subkey中的kv标识。
                            // todo（chen）subkey标识更新了，也需要持久化
                            tmp_bucket->counter[i].subkey = REMOVE_NODE_FLAG(tmp_bucket->counter[i].subkey);
                            tmp_bucket->counter[i].value = (uint64_t) newNode;
                            clflush((char *) &tmp_bucket->counter[i].value, 8);
                            return;
                        }
                    } else {
                        // next is next extendible hash
                        // next是节点，则在节点中再递归处理，这里结束后重新进入循环
                        currentNode = (ERTIntNode *) next;
                        beforeAddress = beforeA;
                        headerDepth = currentNode->header.depth;
                    }
                }
            }
        } else {
            // if prefix is not match (shorter)
            // split a new tree node and insert
            // 前缀不能完全匹配，那么就要从能完全匹配的地方split，未匹配的放到新节点中，作为老节点的子节点
            // build new tree node
            ERTIntNode *newNode = NewERTIntNode(ERT_NODE_LENGTH, headerDepth);
            // 共同匹配的部分构建新节点，原节点因为公共前缀更长所以后面加入到新节点的bucket中作为字节点。
            newNode->header.init(&currentNode->header, matchedPrefixLen, currentNode->header.depth);

            // 迁移treeNodeValues的数据，超过部分加入到split的新节点
            // 因为新节点公共前缀变短了，如果是正向存储，那么 0~matchedPrefixLen-1 的数据都会放到新节点吗。
            // 这里是逆向存储，匹配到matchedPrefixLen的value存储在 header.len-matchedPrefixLen 处
            // 所以从 header.len-matchedPrefixLen 开始，一直到 currentNode->header.len-1
            // TODO（chen）这里应该不需要<=，因为header.len位置表示前缀一点都不匹配的数据，这种会在父节点完全匹配的，当前节点不用考虑。
            for (int j = currentNode->header.len - matchedPrefixLen; j <= currentNode->header.len; j++) {
                newNode->treeNodeValues[j - currentNode->header.len +
                                        matchedPrefixLen] = currentNode->treeNodeValues[j];
            }

            // 新节点对应的bucket key即为能完全匹配的那一部分子key
            uint64_t subkey = GET_SUBKEY(key, (len + matchedPrefixLen) * SIZE_OF_CHAR, ERT_NODE_LENGTH);

            ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
            clflush((char *) kv, sizeof(ERTIntKeyValue));

            // 当前插入的kv加入到新节点
            newNode->put(subkey, (uint64_t) kv, (uint64_t) &newNode);
            // newNode->put(GET_16BITS(currentNode->header.array,matchedPrefixLen),(uint64_t)currentNode);
            // matchedPrefixLen处切分，去除新节点能匹配的部分，后面子串就变为老节点的开始。取得后面子串，将老节点加入作为子节点。
            newNode->put(GET_32BITS(currentNode->header.array, matchedPrefixLen), (uint64_t) currentNode,
                         (uint64_t) &newNode);

            // modify currentNode
            currentNode->header.depth -= matchedPrefixLen * SIZE_OF_CHAR / ERT_NODE_LENGTH;
            currentNode->header.len -= matchedPrefixLen;
            // matchedPrefixLen前面部分在 newNode->header.init 中写到了新节点中，这里老节点中需要移除前面部分，并把后面部分往前挪。
            for (int i = 0; i < ERT_NODE_PREFIX_MAX_BYTES - matchedPrefixLen; i++) {
                currentNode->header.array[i] = currentNode->header.array[i + matchedPrefixLen];
            }
            clflush((char *) &(currentNode->header), 8);
            clflush((char *) newNode, sizeof(ERTIntNode));

            // modify the successor
            *(ERTIntNode **) beforeAddress = newNode;
            return;
        }
    }
}

uint64_t ERTInt::Search(uint64_t key) {
    auto currentNode = root;
    if (currentNode == NULL) {
        return 0;
    }
    int pos = 0;
    while (pos < ERT_KEY_LENGTH / SIZE_OF_CHAR) {
        if (currentNode->header.len) {
            if (ERT_KEY_LENGTH / SIZE_OF_CHAR - pos <= currentNode->header.len) {
                // TODO（chen） ERT_KEY_LENGTH 不需要除以 SIZE_OF_CHAR 吗？
                if (currentNode->treeNodeValues[currentNode->header.len - ERT_KEY_LENGTH + pos].key == key) {
                    return (uint64_t) currentNode->treeNodeValues[currentNode->header.len - ERT_KEY_LENGTH + pos].value;
                } else {
                    return 0;
                }
            }
            // 判断key的pos位置之后len长度数据能否与header.array完全匹配，能匹配则找下一个节点，不能匹配则说明当前key不存在。
            if (!isSame((unsigned char *) currentNode->header.array, key, pos * SIZE_OF_CHAR,
                        currentNode->header.len * SIZE_OF_CHAR)) {
                return 0;
            }
            // 能匹配，往后面找，pos往后挪len长度。
            pos += currentNode->header.len;
        }
        // currentNode->get方法: dir -> segment -> bucket -> item
        // uint64_t subkey = GET_16BITS(key,pos);
        uint64_t subkey = GET_SUBKEY(key, pos * SIZE_OF_CHAR, ERT_NODE_LENGTH);
        bool keyValueFlag = false;
        auto next = currentNode->get(subkey, keyValueFlag);
        // pos+=_16_BITS_OF_BYTES;
        // pos跳过subkey的长度（所以每个节点跳过 前缀+subkey 长度？）
        pos += _32_BITS_OF_BYTES;
        // 对应key没有匹配到，返回0
        if (next == 0) {
            return 0;
        }
        if (keyValueFlag) {
            // keyValueFlag代表是直接存储kv，不是节点。
            // is value
            if (pos == 8) {
                // key长度是8，这里达到长度，直接返回就是value
                return next;
            } else {
                // 长度没有达到key长度，但是是直接存储kv，所以next直接存储的kv对，解析返回。
                if (key == ((ERTIntKeyValue *) next)->key) {
                    return ((ERTIntKeyValue *) next)->value;
                } else {
                    return 0;
                }
            }
        } else {
            // bucket找到的next指向新节点，那么在新节点中继续寻找。pos前面已经更新过了。
            currentNode = (ERTIntNode *) next;
        }
    }
    return 0;
}

// 找寻 left~right 区间的元素，放到res中
void ERTInt::scan(uint64_t left, uint64_t right) {
    vector<ERTIntKeyValue> res;
    nodeScan(root, left, right, res, 0);
//    cout << res.size() << endl;
}


// TODO（chen）这个里scan数据怎么没看到处理 treeNodeValues 数据
void ERTInt::nodeScan(ERTIntNode *tmp, uint64_t left, uint64_t right, vector<ERTIntKeyValue> &res, int pos,
                      uint64_t prefix) {
    if (unlikely(tmp == NULL)) {
        tmp = root;
    }
    // 在当前节点的dir列表中找到匹配区间left的最左位置，和匹配区间right最右位置
    uint64_t leftPos = UINT64_MAX, rightPos = UINT64_MAX;

    // 先在header.array中匹配前缀，与左边界比较。
    for (int i = 0; i < tmp->header.len; i++) {
        uint64_t subkey = GET_SUBKEY(left, pos + i, 8);
        if (subkey == (uint64_t) tmp->header.array[i]) {
            // 如果按字节匹配，则继续往前
            continue;
        } else {
            if (subkey > (uint64_t) tmp->header.array[i]) {
                // 不匹配，subkey前缀更大，显然当前节点都比subkey小，故左边界left不在当前节点，返回。
                return;
            } else {
                // 找到了subkey比节点前缀小，所以subkey一定比所有dir中小，leftPos设为dir idx 0。
                leftPos = 0;
                break;
            }
        }
    }

    // 先在header.array中匹配前缀，与右边界比较。
    for (int i = 0; i < tmp->header.len; i++) {
        uint64_t subkey = GET_SUBKEY(right, pos + i, 8);
        if (subkey == (uint64_t) tmp->header.array[i]) {
            continue;
        } else {
            if (subkey > (uint64_t) tmp->header.array[i]) {
                // right的subkey比前缀大，所以dir所有数据都在区间内，rightPos设为dir_size - 1
                rightPos = tmp->dir_size - 1;
                break;
            } else {
                return;
            }
        }
    }

    // 传入的原前缀prefix加上当前节点的header.array保存的前缀，构建新的前缀
    if (tmp->header.len > 0) {
        prefix = (prefix << tmp->header.len * SIZE_OF_CHAR);
        for (int i = 0; i < tmp->header.len; ++i) {
            prefix += tmp->header.array[i] << (tmp->header.len - i);
        }
        pos += tmp->header.len * SIZE_OF_CHAR;
    }
    // 如果left前缀与当前节点完全匹配，则leftPos == UINT64_MAX，此时需要用leftSubkey定位dir idx
    // 同理right也一样。
    uint64_t leftSubkey = UINT64_MAX, rightSubkey = UINT64_MAX;
    if (leftPos == UINT64_MAX) {
        leftSubkey = GET_SUBKEY(left, pos, ERT_NODE_LENGTH);
        leftPos = GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH, tmp->global_depth);
    }
    if (rightPos == UINT64_MAX) {
        rightSubkey = GET_SUBKEY(right, pos, ERT_NODE_LENGTH);
        rightPos = GET_SEG_NUM(rightSubkey, ERT_NODE_LENGTH, tmp->global_depth);
    }
    // 前缀左移 ERT_NODE_LENGTH，后面加上bucket中找到的subkey，形成的新key即为查询遍历到的key
    prefix = (prefix << ERT_NODE_LENGTH);
    pos += ERT_NODE_LENGTH;
    // TODO（chen）这里应该加一个条件 leftSubkey != UINT64_MAX，因为当leftPos和rightPos都不是max时，这俩都为初值max。
    // TODO（chen）这里可以直接讲 leftPos == rightPos 作为更大的特例处理，包含了leftSubkey == rightSubkey。
    // 此时leftPos和rightPos分别为0和dir_size-1，整个节点全都在scan范围内。
    if (leftSubkey == rightSubkey) {
        // 如果 leftSubkey == rightSubkey，相当于定点取一个位置。
        bool keyValueFlag;
        uint64_t dir_index = GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH, tmp->global_depth);
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, dir_index);
        uint64_t seg_index = GET_BUCKET_NUM(leftSubkey, ERT_BUCKET_MASK_LEN);
        ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
        uint64_t value = tmp_bucket->get(leftSubkey, keyValueFlag);
        // local depth取到的seg跟global_depth渠道的seg理论上应该时一样的，不一样就是程序问题。
        if (value == 0 || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH,
                                                                                       tmp_seg->depth)))) {
            return;
        }
        // 因为leftSubkey == rightSubkey，只有一个值，所以这里不用考虑多值情况
        if (pos == 64) {
            // 达到了完整的key长度，value直接就是值
            ERTIntKeyValue tmp;
            // TODO（chen）这里应该是 prefix + leftSubkey 吧
            tmp.key = leftSubkey + prefix;
            tmp.value = value;
            // 加入到结果中
            res.push_back(tmp);
        } else {
            // 没到完整的key长度
            if (keyValueFlag) {
                // keyValueFlag表示value是kv对象，直接加入，只有一个值，不用考虑多值情况
                res.push_back(*(ERTIntKeyValue *) value);
            } else {
                // value不是kv对象，是指向下一个节点的指针。向下一个节点递归查找。
                nodeScan((ERTIntNode *) value, left, right, res, pos, prefix + leftSubkey);
            }
        }
        return;
    }
    // 遍历[leftPos, rightPos]之间的segment处理。
    ERTIntSegment *last_seg = NULL;
    for (uint32_t i = leftPos; i <= rightPos; i++) {
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            // 因为dir有可能连续几个slot都指向同一个segment，这里跳过连续的。
            continue;
        else
            last_seg = tmp_seg;
        //todo if leftsubkey == rightsubkey, there is no need to scan all the segment.
        // 遍历segment中的bucket
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            // 遍历bucket中的item
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                // 跳过无效的item，因为segment split时，迁移的数据在原节点中没有删除（减少PM写），
                // 所以这里遍历需要判断当前subkey的seg是否跟当前segment一致，不一致则说明是迁移出去的数据，跳过。
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                if ((value == 0 && curSubkey == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp,
                                                                                                  GET_SEG_NUM(curSubkey,
                                                                                                              ERT_NODE_LENGTH,
                                                                                                              tmp_seg->depth)))) {
                    continue;
                }
                if ((leftSubkey == UINT64_MAX || curSubkey > leftSubkey) &&
                    (rightSubkey == UINT64_MAX || curSubkey < rightSubkey)) {
                    // curSubkey 在 leftSubkey 和 rightSubkey 之间，处理结果加入res
                    if (pos == 64) {
                        // 达到了最大键长，value就是值
                        ERTIntKeyValue tmp;
                        // TODO（chen）同前面，这里为啥不是 prefix + curSubkey
                        tmp.key = curSubkey + prefix;
                        tmp.value = value;
                        res.push_back(tmp);
                    } else {
                        if (keyValueFlag) {
                            // 没达到最大键长，keyValueFlag标识，value是kv对
                            res.push_back(*(ERTIntKeyValue *) value);
                        } else {
                            // 没达到最大键长，不是kv对，直接往下面节点找。后面节点都是范围内数据，所以直接遍历取。
                            getAllNodes((ERTIntNode *) value, res, prefix + curSubkey);
                        }
                    }
                } else if (curSubkey == leftSubkey || curSubkey == rightSubkey) {
                    // 边界处理，如果达到了键长，或者有keyValueFlag标识，说明直接指向kv，需要加入结果。
                    if (pos == 64) {
                        ERTIntKeyValue tmp;
                        // TODO（chen）同前面，这里为啥不是 prefix + curSubkey
                        tmp.key = curSubkey + prefix;
                        tmp.value = value;
                        res.push_back(tmp);
                    } else {
                        // TODO（chen）pos == ERT_KEY_LENGTH 这里不需要
                        if (pos == ERT_KEY_LENGTH || keyValueFlag) {
                            res.push_back(*(ERTIntKeyValue *) value);
                        } else {
                            // 没有达到键长，也不是kv标识，指向下一个节点，那么下面节点不一定全都在查询范围内，需要递归再判断
                            nodeScan((ERTIntNode *) value, left, right, res, pos, prefix + curSubkey);
                        }
                    }
                }
            }
        }
    }


}

// 遍历取出指定节点所有数据
void ERTInt::getAllNodes(ERTIntNode *tmp, vector<ERTIntKeyValue> &res, int pos, uint64_t prefix) {
    if (tmp == NULL) {
        return;
    }
    // prefix左移当前节点前缀长度位数
    if (tmp->header.len > 0) {
        prefix = (prefix << tmp->header.len * SIZE_OF_CHAR);
        for (int i = 0; i < tmp->header.len; ++i) {
            prefix += tmp->header.array[i] << (tmp->header.len - i);
        }
        pos += tmp->header.len * SIZE_OF_CHAR;
    }
    // prefix左移当前节点长度位数
    prefix = (prefix << ERT_NODE_LENGTH);
    pos += ERT_NODE_LENGTH;
    ERTIntSegment *last_seg = NULL;
    // 遍历所有的dir slot
    for (int i = 0; i < tmp->dir_size; i++) {
        // 连续的相同segment跳过
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            continue;
        else
            last_seg = tmp_seg;
        // 遍历segment中的bucket
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            // 遍历bucket中的item
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                // item提取kv和flag
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                // kv判断处理
                if (pos == 64) {
                    // 处理达到了key长度，value即为值
                    ERTIntKeyValue tmp;
                    tmp.key = curSubkey + prefix;
                    tmp.value = value;
                    res.push_back(tmp);
                } else {
                    // 跳过无效的item，同前面一样还需要对比segment一致，不一致跳过
                    if ((curSubkey == 0 && value == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(
                            curSubkey, ERT_NODE_LENGTH, tmp_seg->depth)))) {
                        continue;
                    }
                    if (keyValueFlag) {
                        // kv flag，直接加入
                        res.push_back(*(ERTIntKeyValue *) value);
                    } else {
                        // 非kv，往后面节点找，节点数据都加入，递归调用getAllNodes
                        getAllNodes((ERTIntNode *) value, res, pos, prefix + curSubkey);
                    }
                }
            }
        }
    }
}

// 计算内存用量
uint64_t ERTInt::memory_profile(ERTIntNode *tmp, int pos) {
    if (tmp == NULL) {
        tmp = root;
    }
    // 当前节点的dir数量个指针，每个指针8字节，然后加上节点大小
    // TODO（chen）treeNodeValues使用内存没加？
    uint64_t res = tmp->dir_size * 8 + sizeof(ERTIntNode);
    pos += ERT_NODE_LENGTH;
    ERTIntSegment *last_seg = NULL;
    // 遍历dir列表，处理segment
    for (int i = 0; i < tmp->dir_size; i++) {
        // 跳过相同的segment
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            continue;
        else {
            last_seg = tmp_seg;
            // 每个segment里有ERT_MAX_BUCKET_NUM 个bucket
            // TODO（chen）每个segment这里还需要加上 sizeof(ERTIntSegment) ？
            res += ERT_MAX_BUCKET_NUM * sizeof(ERTIntBucket);
        }
        // 遍历segment中的bucket
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            // 遍历bucket中的item
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                // 如果达到了key的长度，value直接存储在item的value中。前面sizeof(ERTIntBucket)包含了存储占用，这里就不需要计算了
                if (pos != 64) {
                    // 过滤无效item，因为无效item如果是指针，那么指针对象如果有效会在其他节点计算的
                    if ((curSubkey == 0 && value == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(
                            curSubkey, ERT_NODE_LENGTH, tmp_seg->depth)))) {
                        continue;
                    }
                    if (keyValueFlag) {
                        // 指针指向的kv对，+sizeof(ERTIntBucketKeyValue)，也就是+16
                        res += 16;
                    } else {
                        // 递归调用value指向的节点，计算占用存储空间
                        res += memory_profile((ERTIntNode *) value, pos);
                    }
                }
            }
        }
    }
    return res;
}

// 创建扩展hash的基数树，并初始化
ERTInt *NewExtendibleRadixTreeInt() {
    // 创建tree
    ERTInt *_new_hash_tree = static_cast<ERTInt *>(concurrency_fast_alloc(sizeof(ERTInt)));
    // 初始化创建根节点，root指向根节点
    _new_hash_tree->init();
    return _new_hash_tree;
}


ERTInt::ERTInt() {
    root = NewERTIntNode(ERT_NODE_LENGTH);
}

ERTInt::ERTInt(int _span, int _init_depth) {
    init_depth = _init_depth;
    root = NewERTIntNode(ERT_NODE_LENGTH);
}

ERTInt::~ERTInt() {
    delete root;
}

void ERTInt::init() {
    root = NewERTIntNode(ERT_NODE_LENGTH);
}
#define MVB_MAX_SIZE 4096*2 
#define D 0.0f               
#define EPSILON 1.00f



#include "../containers/btree.h"
#include "../def_global.h"
#include <stack>
#include <vector>
#include <map>
#include <set>
#include <algorithm> 
#include <cstring>   
#include <chrono>

#define MVBTREE_PRINT(msg) std::cout << msg << std::endl;
#define MVBTREE_ASSERT(expr, msg) assert((expr) && (msg));
#define MVBTREE_MAX(a, b) ((a) < (b) ? (b) : (a))

const bool USE_MAX_KEY = true;
const bool COPY_ENTRY_WITH_SEGMENT = true;
const bool PRINT_QUERY_NODES = false;

namespace stx
{

    template <typename _Version, typename _Key, typename _Data>
    class mvbtree_default_map_traits
    {
    public:

        static const int leaf_slots = MVBTREE_MAX(8, MVB_MAX_SIZE / (sizeof(_Key) + 2 * sizeof(_Version) + sizeof(_Data) + sizeof(bool)));
        static const int inner_slots = USE_MAX_KEY ? MVBTREE_MAX(8, MVB_MAX_SIZE / (2 * sizeof(_Key) + 2 * sizeof(_Version) + sizeof(void *) + sizeof(bool))) : MVBTREE_MAX(8, MVB_MAX_SIZE / (sizeof(_Key) + 2 * sizeof(_Version) + sizeof(void *) + sizeof(bool)));
    };

    template <typename _Version, typename _Key, typename _Data,
              typename _Value = std::pair<_Key, _Data>,
              typename _VersionCompare = std::less<_Version>,
              typename _KeyCompare = std::less<_Key>,
              typename _Traits = mvbtree_default_map_traits<_Version, _Key, _Data>,
              bool _Duplicates = false,
              typename _Alloc = std::allocator<_Value>,
              bool _UsedAsSet = false>
    class mvbtree
    {
#pragma region typedef
    public:

        typedef _Version version_type;
        typedef _Key key_type;
        typedef _Data data_type;
        typedef _Value value_type;
        typedef _VersionCompare version_compare;
        typedef _KeyCompare key_compare;
        typedef _Traits traits;
        static const bool allow_duplicates = _Duplicates;
        typedef _Alloc allocator_type;
        static const bool used_as_set = _UsedAsSet;

    public:

        typedef mvbtree<version_type, key_type, data_type, value_type, version_compare, key_compare,
                        traits, allow_duplicates, allocator_type, used_as_set>
            self_type;

        typedef size_t size_type;
        typedef uint64_t id_type;
        typedef std::pair<key_type, data_type> pair_type;

    public:

        static const unsigned short max_inner_slot_size = traits::inner_slots;
        static const unsigned short min_alive_inner_slot_size = (max_inner_slot_size * D);
        static const unsigned short strong_max_alive_inner_slot_size = (max_inner_slot_size * (1.0f - D * EPSILON));
        static const unsigned short strong_min_alive_inner_slot_size = (max_inner_slot_size * D * (1.0f + EPSILON));
        static const unsigned short max_leaf_slot_size = traits::leaf_slots;
        static const unsigned short min_alive_leaf_slot_size = (max_leaf_slot_size * D);
        static const unsigned short strong_max_alive_leaf_slot_size = (max_leaf_slot_size * (1.0f - D * EPSILON));
        static const unsigned short strong_min_alive_leaf_slot_size = (max_leaf_slot_size * D * (1.0f + EPSILON));

#pragma endregion

#pragma region basic
    public:
        struct Lifespan
        {
            version_type start_version;

            version_type end_version;

            inline Lifespan(const version_type s, const version_type e = std::numeric_limits<version_type>::max())
                : start_version(s), end_version(e)
            {
            }

            void set_start_version(version_type startVersion)
            {
                start_version = startVersion;
            }

            inline void end(const version_type &e)
            {
                MVBTREE_ASSERT(is_alive(), "Only the alive lifespan can die.");
                end_version = e;
            }

            inline bool is_alive() const
            {
                return (end_version == std::numeric_limits<version_type>::max());
            }

            bool operator==(Lifespan const &comp_lifespan)
            {
                return (start_version == comp_lifespan.start_version) && (end_version == comp_lifespan.end_version);
            }

            inline std::string to_str() const
            {
                if (is_alive())
                {
                    return ("[ " + std::to_string(start_version) + ", * )");
                }
                else
                {
                    return ("[ " + std::to_string(start_version) + ", " + std::to_string(end_version) + " )");
                }
            }
        };

        struct KeyRange
        {
            key_type min_key;

            key_type max_key;

            inline KeyRange(const key_type min, const key_type max = std::numeric_limits<key_type>::max())
                : min_key(min), max_key(max)
            {
            }

            inline void set_max_key(const key_type &max)
            {
                MVBTREE_ASSERT(max > max_key, "Only set max_key to a larger value.");
                MVBTREE_ASSERT(max >= min_key, "The new max_key should not be smaller than min_key.");

                max_key = max;
            }

            inline std::string to_str() const
            {
                return ("[ " + std::to_string(min_key) + ", " + std::to_string(max_key) + " ]");
            }
        };

    public:

        struct Node;

    public:
        struct Entry
        {
            key_type key;
            mutable Lifespan lifespan;
            bool entry_type;

            inline Entry(const key_type k, const Lifespan l)
                : key(k), lifespan(l), entry_type(true)
            {
            }

            inline void end(const version_type &end_version)
            {
                lifespan.end(end_version);
            }

            inline bool is_alive() const
            {
                return (lifespan.is_alive());
            }

            void set_entry_type(bool changed_entry_type)
            {
                entry_type = changed_entry_type;
            }
        };

        struct InnerEntry : public Entry
        {
            Node *ptr_child;

            key_type max_key;

            inline InnerEntry() 
                : Entry(std::numeric_limits<key_type>::min(), std::numeric_limits<version_type>::min()),
                  ptr_child(NULL), max_key(std::numeric_limits<key_type>::max())
            {
            }

            inline InnerEntry(const KeyRange r, const version_type s, Node *p)
                : Entry(r.min_key, s), ptr_child(p), max_key(r.max_key)
            {
            }

            inline std::string entry_to_str() const
            {
                std::string entry_type_str = Entry::entry_type ? "positive" : "negative";

                return ("keyrange [" + std::to_string(Entry::key) + ", " + std::to_string(max_key) + ")" + ", " + "lifespan " + Entry::lifespan.to_str() + ", type " + entry_type_str + ", pointer->id " + std::to_string(ptr_child->id));
            }
        };

        struct LeafEntry : public Entry
        {
            key_type id = std::numeric_limits<key_type>::min();

            inline LeafEntry() 
                : Entry(std::numeric_limits<key_type>::min(), std::numeric_limits<version_type>::min()), id(std::numeric_limits<key_type>::min())
            {
            }

            inline LeafEntry(const key_type k, const version_type s, key_type p) : Entry(k, s), id(p)
            {
            }

            bool operator==(LeafEntry const &comp_entry)
            {
                return (Entry::key == comp_entry.key) && (Entry::lifespan == comp_entry.lifespan);
            }

            inline std::string entry_to_str() const
            {
                std::string entry_type_str = Entry::entry_type ? "positive" : "negative";

                return ("key " + std::to_string(Entry::key) + ", lifespan " + Entry::lifespan.to_str() + ", type " + entry_type_str + ", id " + std::to_string(id));
            }
        };

    public:

        struct Node
        {
            id_type id;

            unsigned short level;

            unsigned short used_slot_size;

            unsigned short alive_slot_size; 
            KeyRange keyrange;

            Lifespan lifespan;

            inline Node(const unsigned short l, const KeyRange r, const version_type s)
                : level(l), used_slot_size(0), alive_slot_size(0), keyrange(r), lifespan(s)
            {
            }

            inline void end_node(const version_type &end_version)
            {
                lifespan.end(end_version);
                alive_slot_size = 0;
            }

            inline bool is_alive() const
            {
                return (lifespan.is_alive());
            }

            inline bool is_leaf() const
            {
                return (level == 0);
            }

            inline std::string boundary_str(version_type current_version, key_type maximum_key) const
            {
                std::string info = "";
                info += std::to_string(level);
                info += "," + std::to_string(lifespan.start_version);
                info += "," + std::to_string(keyrange.min_key);
                info += ",";
                info += lifespan.end_version == std::numeric_limits<version_type>::max() ? std::to_string(current_version - lifespan.start_version) : std::to_string(lifespan.end_version - lifespan.start_version);
                info += ",";
                info += keyrange.max_key == std::numeric_limits<key_type>::max() ? std::to_string(maximum_key - keyrange.min_key) : std::to_string(keyrange.max_key - keyrange.min_key);

                return info;
            }

            inline std::string node_to_str() const
            {
                std::string info = "";
                info += "id " + std::to_string(id);
                info += " level " + std::to_string(level);
                info += " keyrange " + keyrange.to_str();
                info += " lifespan " + lifespan.to_str();
                info += " used_slot_size " + std::to_string(used_slot_size);
                info += " alive_slot_size " + std::to_string(alive_slot_size);

                return info;
            }

            inline void print() const
            {
                // MVBTREE_PRINT(node_to_str());
            }
        };


        struct InnerNode : public Node
        {
            typedef typename _Alloc::template rebind<InnerNode>::other alloc_type;

            InnerEntry entries[max_inner_slot_size];

            InnerNode(const unsigned short l, const KeyRange r, const version_type s, InnerEntry *entries)
                : Node(l, r, s), entries(entries) {}

            inline InnerNode(const unsigned short l, const KeyRange r, const version_type s)
                : Node(l, r, s) {}

            inline bool is_block_overflow(int number_of_changed_alive_slots = 0) const
            {
                MVBTREE_ASSERT(number_of_changed_alive_slots >= 0, "The number_of_changed_alive_slots should be non-negative.");

                return ((Node::used_slot_size + number_of_changed_alive_slots) > max_inner_slot_size);
            }

            inline bool is_weak_version_underflow(int number_of_changed_alive_slots = 0) const
            {
                MVBTREE_ASSERT(number_of_changed_alive_slots < 0, "The number_of_changed_alive_slots should be negative.");

                return ((Node::alive_slot_size + number_of_changed_alive_slots) < min_alive_inner_slot_size);
            }

            inline bool is_strong_version_overflow(int number_of_changed_alive_slots = 0) const
            {
                return ((Node::alive_slot_size + number_of_changed_alive_slots) > strong_max_alive_inner_slot_size);
            }

            inline bool is_strong_version_underflow(int number_of_changed_alive_slots = 0) const
            {
                return ((Node::alive_slot_size + number_of_changed_alive_slots) < strong_min_alive_inner_slot_size);
            }

            inline void insert_entry(const InnerEntry &entry)
            {
                MVBTREE_ASSERT(entry.is_alive(), "The inserted entry should be alive.");
                MVBTREE_ASSERT(!is_block_overflow(1), "The inserted entry should not cause block overflow.");

                entries[Node::used_slot_size] = entry;
                Node::used_slot_size++;
                Node::alive_slot_size++;
            }

            inline void end_entry(int slot, const version_type &end_version)
            {
                entries[slot].end(end_version);
                Node::alive_slot_size--;
            }

            inline void empty_entries()
            {
                memset(entries, 0, sizeof(entries));
                Node::used_slot_size = 0;
                Node::alive_slot_size = 0;
            }

            inline void fill_entries(const std::vector<InnerEntry> &v_entries)
            {
                MVBTREE_ASSERT(Node::used_slot_size == 0, "Only empty entries can be filled.");
                MVBTREE_ASSERT(v_entries.size() <= max_inner_slot_size, "The size of inserted entries should not be larger than max_inner_slot_size.");

                std::copy(v_entries.begin(), v_entries.end(), entries);
                Node::used_slot_size = v_entries.size();
                Node::alive_slot_size = v_entries.size();
            }

            inline std::string node_to_str(bool full = false) const
            {
                std::string info = Node::node_to_str();
                if (full)
                {
                    info += "\nInner Entries: \n";
                    for (int i = 0; i < Node::used_slot_size; i++)
                    {
                        info += entries[i].entry_to_str() + "\n";
                    }
                }
                return info;
            }

            inline void print(bool full = false) const
            {
                // MVBTREE_PRINT(node_to_str(full));
            }
        };


        struct LeafNode : public Node
        {
            typedef typename _Alloc::template rebind<LeafNode>::other alloc_type;

            LeafEntry entries[max_leaf_slot_size];

            LeafNode(const unsigned short l, const KeyRange r, const version_type s, LeafEntry *entries)
                : Node(l, r, s), entries(entries) {}

            inline LeafNode(const KeyRange r, const version_type s)
                : Node(0, r, s) {}

            inline bool is_block_overflow(int number_of_changed_alive_slots = 0) const
            {
                MVBTREE_ASSERT(number_of_changed_alive_slots >= 0, "The number_of_changed_alive_slots should be non-negative.");

                return ((Node::used_slot_size + number_of_changed_alive_slots) > max_leaf_slot_size);
            }

            inline bool is_weak_version_underflow(int number_of_changed_alive_slots = 0) const
            {
                MVBTREE_ASSERT(number_of_changed_alive_slots < 0, "The number_of_changed_alive_slots should be negative.");

                return ((Node::alive_slot_size + number_of_changed_alive_slots) < min_alive_leaf_slot_size);
            }

            inline bool is_strong_version_overflow(int number_of_changed_alive_slots = 0) const
            {
                return ((Node::alive_slot_size + number_of_changed_alive_slots) > strong_max_alive_leaf_slot_size);
            }

            inline bool is_strong_version_underflow(int number_of_changed_alive_slots = 0) const
            {
                return ((Node::alive_slot_size + number_of_changed_alive_slots) < strong_min_alive_leaf_slot_size);
            }

            inline void insert_entry(const LeafEntry &entry)
            {
                MVBTREE_ASSERT(entry.is_alive(), "The inserted entry should be alive.");
                MVBTREE_ASSERT(!is_block_overflow(1), "The inserted entry should not cause block overflow.");

                entries[Node::used_slot_size] = entry;
                Node::used_slot_size++;
                Node::alive_slot_size++;
            }

            inline void end_entry(int slot, const version_type &end_version)
            {
                entries[slot].end(end_version);
                Node::alive_slot_size--;
            }

            inline void empty_entries()
            {
                memset(entries, 0, sizeof(entries));
                Node::used_slot_size = 0;
                Node::alive_slot_size = 0;
            }

            inline void fill_entries(const std::vector<LeafEntry> &v_entries)
            {
                MVBTREE_ASSERT(Node::used_slot_size == 0, "Only empty entries can be filled.");
                MVBTREE_ASSERT(v_entries.size() <= max_leaf_slot_size, "The size of inserted entries should not be larger than max_inner_slot_size.");

                std::copy(v_entries.begin(), v_entries.end(), entries);
                Node::used_slot_size = v_entries.size();
                Node::alive_slot_size = v_entries.size();
            }

            inline std::string node_to_str(bool full = false) const
            {
                std::string info = Node::node_to_str();
                if (full)
                {
                    info += "\nLeaf Entries: \n";
                    for (int i = 0; i < Node::used_slot_size; i++)
                    {
                        info += entries[i].entry_to_str() + "\n";
                    }
                }
                return info;
            }

            inline void print(bool full = false) const
            {
                // MVBTREE_PRINT(node_to_str(full));
            }
        };

    private:
        struct RootBox
        {
            Lifespan lifespan;

            KeyRange keyrange;

            Node *ptr_root;

            inline RootBox()
                : lifespan(std::numeric_limits<version_type>::min()),
                  keyrange(std::numeric_limits<key_type>::min(), std::numeric_limits<key_type>::min()), ptr_root(NULL)
            {
            }

            inline RootBox(const KeyRange r, const version_type s, Node *p)
                : lifespan(s), keyrange(r.min_key, r.max_key), ptr_root(p)
            {
            }

            inline bool is_alive() const
            {
                return (lifespan.is_alive());
            }

            inline void end(const version_type &end_version)
            {
                lifespan.end(end_version);
            }

            inline void set_max_key(const key_type &max_key)
            {
                keyrange.set_max_key(max_key);
            }

            inline std::string to_str() const
            {
                return ("lifespan " + lifespan.to_str() + ", keyrange " + keyrange.to_str() + ", ptr_root -> id " + std::to_string(ptr_root->id) + ", level " + std::to_string(ptr_root->level));
            }
        };

    public:

        struct Stats
        {
            size_type item_size;

            size_type leaf_node_size;

            size_type inner_node_size;

            size_type alive_leaf_node_size; 

            size_type alive_inner_node_size; 

            static const unsigned short leaf_slot_size = max_leaf_slot_size;

            static const unsigned short inner_slot_size = max_inner_slot_size;

            inline Stats()
                : item_size(0), leaf_node_size(0), inner_node_size(0), alive_leaf_node_size(0), alive_inner_node_size(0)
            {
            }

            inline size_type node_size() const
            {
                return leaf_node_size + inner_node_size;
            }

            inline double leaf_node_avg_fill() const
            {
                return static_cast<double>(item_size) / (leaf_node_size * leaf_slot_size);
            }

            inline std::string to_str() const
            {
                std::string stats_info = "";
                stats_info += "item_size " + std::to_string(item_size);
                stats_info += "\ninner_node_size " + std::to_string(inner_node_size);
                stats_info += " alive_inner_node_size " + std::to_string(alive_inner_node_size);
                stats_info += " inner_slot_size " + std::to_string(inner_slot_size);
                stats_info += "\nleaf_node_size " + std::to_string(leaf_node_size);
                stats_info += " alive_leaf_node_size " + std::to_string(alive_leaf_node_size);
                stats_info += " leaf_slot_size " + std::to_string(leaf_slot_size);
                stats_info += " leaf_node_avg_fill " + std::to_string(leaf_node_avg_fill());

                return stats_info;
            }
        };

#pragma endregion

#pragma region compare
    private:

        inline bool key_less(const key_type &a, const key_type &b) const
        {
            return m_key_less(a, b);
        }

        inline bool key_lessequal(const key_type &a, const key_type &b) const
        {
            return !m_key_less(b, a);
        }

        inline bool key_greater(const key_type &a, const key_type &b) const
        {
            return m_key_less(b, a);
        }

        inline bool key_greaterequal(const key_type &a, const key_type &b) const
        {
            return !m_key_less(a, b);
        }

        inline bool key_equal(const key_type &a, const key_type &b) const
        {
            return !m_key_less(a, b) && !m_key_less(b, a);
        }

    private:

        inline bool version_less(const version_type &a, const version_type &b) const
        {
            return m_version_less(a, b);
        }

        inline bool version_lessequal(const version_type &a, const version_type &b) const
        {
            return !m_version_less(b, a);
        }

        inline bool version_greater(const version_type &a, const version_type &b) const
        {
            return m_version_less(b, a);
        }

        inline bool version_greaterequal(const version_type &a, const version_type &b) const
        {
            return !m_version_less(a, b);
        }

        inline bool version_equal(const version_type &a, const version_type &b) const
        {
            return !m_version_less(a, b) && !m_version_less(b, a);
        }

    private:

        struct leaf_entry_key_compare
        {
        protected:
            key_compare key_comp;

        public:
            inline bool operator()(const LeafEntry &e1, const LeafEntry &e2) const
            {
                return key_comp(e1.key, e2.key);
            }
        };

        struct inner_entry_key_compare
        {
        protected:
            key_compare key_comp;

        public:
            inline bool operator()(const InnerEntry &e1, const InnerEntry &e2) const
            {
                return key_comp(e1.key, e2.key);
            }
        };

        struct leaf_entry_version_compare
        {
        protected:
            key_compare key_comp;
            version_compare version_comp;

        public:
            inline bool operator()(const LeafEntry &e1, const LeafEntry &e2) const
            {
                return (e1.lifespan.start_version == e2.lifespan.start_version) ? key_comp(e1.key, e2.key) : version_comp(e1.lifespan.start_version, e2.lifespan.start_version);
            }
        };

        struct inner_entry_version_compare
        {
        protected:
            key_compare key_comp;
            version_compare version_comp;

        public:
            inline bool operator()(const InnerEntry &e1, const InnerEntry &e2) const
            {
                return (e1.lifespan.start_version == e2.lifespan.start_version) ? key_comp(e1.key, e2.key) : version_comp(e1.lifespan.start_version, e2.lifespan.start_version);
            }
        };

    private:

        inline void sort_vector_by_id(std::vector<int> &v_index)
        {
            std::sort(v_index.begin(), v_index.end());
        }

        inline void sort_vector_by_key(std::vector<LeafEntry> &v_entries)
        {
            std::sort(v_entries.begin(), v_entries.end(), leaf_entry_key_compare());
        }

        inline void sort_vector_by_key(std::vector<InnerEntry> &v_entries)
        {
            std::sort(v_entries.begin(), v_entries.end(), inner_entry_key_compare());
        }

        inline void sort_vector_by_version(std::vector<LeafEntry> &v_entries)
        {
            std::sort(v_entries.begin(), v_entries.end(), leaf_entry_version_compare());
        }

        inline void sort_vector_by_version(std::vector<InnerEntry> &v_entries)
        {
            std::sort(v_entries.begin(), v_entries.end(), inner_entry_version_compare());
        }

    public:
        struct lifespan_compare
        {
        protected:
            version_compare version_comp;

        public:
            inline bool operator()(const Lifespan &l1, const Lifespan &l2) const
            {
                return version_comp(l1.start_version, l2.start_version);
            }
        };

#pragma endregion

#pragma region tree
    public:
        stx::btree<version_type, RootBox> m_roots;
        Node *m_root;
        RootBox m_rootbox;
        Stats m_stats;
        version_compare m_version_less;
        key_compare m_key_less;
        allocator_type m_allocator;
        key_type m_min_key = std::numeric_limits<key_type>::min();
        version_type m_current_version = 0;

        inline void set_m_current_version(const version_type &current_version)
        {
            MVBTREE_ASSERT(version_greaterequal(current_version, m_current_version), "The current_version should be non_decreasing.");

            if (current_version > m_current_version)
            {
                m_current_version = current_version;
            }
        }

        inline void update_m_rootbox(const key_type &new_key)
        {
            if (key_greater(new_key, m_rootbox.keyrange.max_key))
                m_rootbox.set_max_key(new_key);
        }

    public:

        explicit inline mvbtree(const allocator_type &alloc = allocator_type())
            : m_root(NULL), m_allocator(alloc), m_current_version(0)
        {
        }

        inline ~mvbtree()
        {
            // MVBTREE_PRINT("\n== Ready to clear mvbtree.");
            // clear();
            // MVBTREE_PRINT("== Success to clear mvbtree.");
        }

        void swap(self_type &from)
        {
            std::swap(m_roots, from.m_roots);
            std::swap(m_rootbox, from.m_rootbox);
            std::swap(m_root, from.m_root);
            std::swap(m_stats, from.m_stats);
            std::swap(m_version_less, from.m_version_less);
            std::swap(m_key_less, from.m_key_less);
            std::swap(m_allocator, from.m_allocator);
        }

    public:
        allocator_type get_allocator() const
        {
            return m_allocator;
        }

    public:
        typename LeafNode::alloc_type leaf_node_allocator()
        {
            return typename LeafNode::alloc_type(m_allocator);
        }

        typename InnerNode::alloc_type inner_node_allocator()
        {
            return typename InnerNode::alloc_type(m_allocator);
        }

        inline id_type get_node_id()
        {
            return m_stats.inner_node_size + m_stats.leaf_node_size;
        }

        inline LeafNode *allocate_leaf_node(const KeyRange keyrange, const version_type start_version)
        {
            LeafNode *n = new (leaf_node_allocator().allocate(1))
                LeafNode(keyrange, start_version);
            n->id = get_node_id();
            m_stats.leaf_node_size++;
            m_stats.alive_leaf_node_size++;
            return n;
        }

        inline InnerNode *allocate_inner_node(unsigned short level, const KeyRange keyrange, const version_type start_version)
        {
            InnerNode *n = new (inner_node_allocator().allocate(1))
                InnerNode(level, keyrange, start_version);
            n->id = get_node_id();
            m_stats.inner_node_size++;
            m_stats.alive_inner_node_size++;
            return n;
        }

        inline void free_node(Node *n)
        {
            if (n->is_leaf())
            {
                if (n->is_alive())
                    m_stats.alive_leaf_node_size--;
                LeafNode *ln = static_cast<LeafNode *>(n);
                typename LeafNode::alloc_type a(leaf_node_allocator());
                a.destroy(ln);
                a.deallocate(ln, 1);
                m_stats.leaf_node_size--;
            }
            else
            {
                if (n->is_alive())
                    m_stats.alive_inner_node_size--;
                InnerNode *in = static_cast<InnerNode *>(n);
                typename InnerNode::alloc_type a(inner_node_allocator());
                a.destroy(in);
                a.deallocate(in, 1);
                m_stats.inner_node_size--;
            }
        }

    public:

        void clear()
        {
            // MVBTREE_PRINT("Before clearing all nodes: " << m_stats.inner_node_size << " inner nodes + " << m_stats.leaf_node_size << " leaf nodes.");

            // if (m_root)
            // {
            //     clear_recursive(m_root);
            //     free_node(m_root);
            //     m_root = NULL;
            // }

            // MVBTREE_PRINT("After clearing nodes under the current root: " << m_stats.inner_node_size << " inner nodes + " << m_stats.leaf_node_size << " leaf nodes.");

            // size_type historical_root_size = 0;
            // if (!m_roots.empty())
            // {
            //     auto v_pairs = m_roots.get_leaf_pairs();
            //     historical_root_size = v_pairs.size();
            //     for (size_type i = 0; i < historical_root_size; i++)
            //     {
            //         Node *historical_root = v_pairs[i].second.ptr_root;
            //         clear_recursive(historical_root);
            //         free_node(historical_root);
            //     }
            // }
            // MVBTREE_PRINT("After clearing nodes under " << historical_root_size << " historical roots: "
            //                                             << m_stats.inner_node_size << " inner nodes + " << m_stats.leaf_node_size << " leaf nodes.");

            // MVBTREE_ASSERT(m_stats.inner_node_size + m_stats.leaf_node_size == 0, "Fail to free some nodes.");

            // m_stats = Stats();
        }

    private:
        void clear_recursive(Node *n)
        {
            if (!n->is_leaf())
            {
                InnerNode *inner_node = static_cast<InnerNode *>(n);

                for (unsigned short slot = 0; slot < inner_node->used_slot_size; ++slot)
                {
                    if (inner_node->entries[slot].entry_type)
                    {
                        clear_recursive(inner_node->entries[slot].ptr_child);
                        free_node(inner_node->entries[slot].ptr_child);
                    }
                }
            }
        }

        void clear_dead_recursive(Node *n)
        {
            if (!n->is_leaf())
            {
                InnerNode *inner_node = static_cast<InnerNode *>(n);

                for (unsigned short slot = 0; slot < inner_node->used_slot_size; ++slot)
                {
                    if (inner_node->entries[slot].entry_type && !inner_node->entries[slot].is_alive())
                    {
                        clear_recursive(inner_node->entries[slot].ptr_child);
                        free_node(inner_node->entries[slot].ptr_child);
                    }
                }
            }
        }

    public:

        inline size_type size() const
        {
            return m_stats.item_size;
        }

        inline bool empty() const
        {
            return (size() == size_type(0));
        }

        inline size_type max_size() const
        {
            return size_type(-1);
        }

        int num_nodes() const
        {
            return m_stats.inner_node_size + m_stats.leaf_node_size;
        }

        int num_leaf_node() const
        {
            return m_stats.leaf_node_size;
        }

        int num_model_node() const
        {
            return m_stats.inner_node_size;
        }

        long long model_size() const
        {
            return m_stats.inner_node_size * MVB_MAX_SIZE;
        }

        long long data_size() const
        {
            return m_stats.leaf_node_size * MVB_MAX_SIZE;
        }

        inline const struct Stats &get_stats() const
        {
            return m_stats;
        }

        inline std::string tree_to_str(Node *root_node) const
        {
            std::string info = "";
            std::stack<Node *> s;
            s.push(root_node);

            while (!s.empty())
            {
                Node *node = s.top();
                s.pop();

                if (node->is_leaf())
                {
                    const LeafNode *leaf_node = static_cast<const LeafNode *>(node);
                    info += "Leaf Node: " + leaf_node->node_to_str();
                }
                else
                {
                    const InnerNode *inner_node = static_cast<const InnerNode *>(node);
                    info += "Inner Node: " + inner_node->node_to_str();
                    for (int i = 0; i < inner_node->used_slot_size; i++)
                    {
                        s.push(inner_node->entries[i].ptr_child);
                    }
                }
            }

            return info;
        }

        inline std::string historical_tree_to_str() const
        {
            std::string info = "";

            if (!m_roots.empty())
            {
                auto v_pairs = m_roots.get_leaf_pairs();
                std::map<id_type, RootBox> print_map;
                for (size_type i = 0; i < v_pairs.size(); i++)
                {
                    Node *historical_root_node = v_pairs[i].second.ptr_root;
                    info += "\nhistorical_root: " + historical_root_node->node_to_str();
                }
            }

            return info;
        }

        inline std::string all_to_str() const
        {
            std::string info = "";
            if (m_stats.item_size > 0)
            {
                info += "m_stats: " + m_stats.to_str() + "\n";
                info += "m_current_version: " + std::to_string(m_current_version) + "\n";
                info += "m_rootbox: " + m_rootbox.to_str() + "\n";
                info += "\nm_root: " + tree_to_str(m_root);
                info += historical_tree_to_str();
            }
            return info;
        }

        inline std::string brief_to_str() const
        {
            std::string info = "";
            if (m_stats.item_size > 0)
            {
                info += "m_stats: " + m_stats.to_str() + "\n";
                info += "m_current_version: " + std::to_string(m_current_version) + "\n";
                info += "m_rootbox: " + m_rootbox.to_str() + "\n";
                info += "m_root: " + m_root->node_to_str() + "\n";
                info += "m_roots(historical): " + std::to_string(m_roots.size());
            }
            return info;
        }

        inline void print() const
        {
            if (m_stats.item_size > 0)
            {
                MVBTREE_PRINT("\n--- Start of the MVB Tree ---\n"
                              << all_to_str() << "\n--- End of the MVB Tree ---\n");
            }
        }

        inline void print_brief() const
        {
            if (m_stats.item_size > 0)
            {
                MVBTREE_PRINT("\n--- Start of the Brief MVB Tree ---\n"
                              << brief_to_str() << "\n--- End of the Brief MVB Tree ---\n");
            }
        }

        inline void get_node_boundary(Node *n, std::vector<std::string> &v_boundary)
        {
            if (!n->is_leaf())
            {
                InnerNode *inner_node = static_cast<InnerNode *>(n);

                for (unsigned short slot = 0; slot < inner_node->used_slot_size; ++slot)
                {
                    if (inner_node->entries[slot].entry_type)
                    {
                        get_node_boundary(inner_node->entries[slot].ptr_child, v_boundary);
                        v_boundary.push_back(inner_node->entries[slot].ptr_child->boundary_str(m_current_version, m_rootbox.keyrange.max_key));
                    }
                }
            }
        }

        inline std::vector<std::string> get_trees_boundary()
        {
            std::vector<std::string> v_boundary;

            if (!m_roots.empty())
            {
                auto v_pairs = m_roots.get_leaf_pairs();
                for (size_type i = 0; i < v_pairs.size(); i++)
                {
                    Node *historical_root_node = v_pairs[i].second.ptr_root;
                    get_node_boundary(historical_root_node, v_boundary);
                    v_boundary.push_back(historical_root_node->boundary_str(m_current_version, m_rootbox.keyrange.max_key));
                }
            }

            get_node_boundary(m_root, v_boundary);
            v_boundary.push_back(m_root->boundary_str(m_current_version, m_rootbox.keyrange.max_key));

            return v_boundary;
        }

#pragma endregion

#pragma region overlap
    private:

        inline bool inner_entry_potentially_contain_key(const InnerEntry &entry, const key_type &key) const
        {
            return key_lessequal(entry.key, key);
        }

        inline bool inner_entry_contain_key(const InnerEntry &entry, const key_type &key) const
        {
            return key_lessequal(entry.key, key) && key_less(key, entry.max_key);
        }

        inline bool inner_entry_contain_key_id(const InnerEntry &entry, const key_type &key, const key_type &id) const
        {
            return key_lessequal(entry.key, key) && key_less(key, entry.max_key);
        }

        inline bool inner_entry_contain_version(const InnerEntry &entry, const version_type &version) const
        {
            return (version_lessequal(entry.lifespan.start_version, version) && version_less(version, entry.lifespan.end_version));
        }

        inline bool inner_entry_overlap_lifespan(const InnerEntry &entry, const Lifespan &lifespan) const
        {
            return (version_less(lifespan.start_version, entry.lifespan.end_version) && version_lessequal(entry.lifespan.start_version, lifespan.end_version));
        }

        inline bool inner_entry_overlap_keyrange(const InnerEntry &entry, const KeyRange &keyrange) const
        {
            return (key_lessequal(keyrange.min_key, entry.max_key) && key_lessequal(entry.key, keyrange.max_key));
        }

        inline bool leaf_entry_contain_key(const LeafEntry &entry, const key_type &key) const
        {
            return key_equal(entry.key, key);
        }

        inline bool leaf_entry_contain_id(const LeafEntry &entry, const key_type &id) const
        {
            return key_equal(entry.id, id);
        }

        inline bool leaf_entry_contain_version(const LeafEntry &entry, const version_type &version) const
        {
            return (version_lessequal(entry.lifespan.start_version, version) && version_less(version, entry.lifespan.end_version));
        }

        inline bool leaf_entry_overlap_lifespan(const LeafEntry &entry, const Lifespan &lifespan) const
        {
            return (version_less(lifespan.start_version, entry.lifespan.end_version) && version_lessequal(entry.lifespan.start_version, lifespan.end_version));
        }

        inline bool leaf_entry_overlap_keyrange(const LeafEntry &entry, const KeyRange &keyrange) const
        {
            return (key_lessequal(keyrange.min_key, entry.key) && key_lessequal(entry.key, keyrange.max_key));
        }

#pragma endregion

#pragma region get_utils
    private:

        int find_inner_entry_index(const InnerNode *node, const key_type &key, const version_type &version, bool filter = false)
        {
            int index = -1;
            int i = 0;

            if (USE_MAX_KEY)
            {
                while (i < node->used_slot_size)
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_contain_key(node->entries[i], key))
                    {
                        index = i;
                        return index;
                    }
                    i++;
                }
            }
            else
            {
                while (i < node->used_slot_size)
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_potentially_contain_key(node->entries[i], key))
                    {
                        if (index == -1)
                        {
                            index = i;
                        }
                        else if (key_greater(node->entries[i].key, node->entries[index].key))
                        {
                            index = i;
                        }
                        MVBTREE_ASSERT(index >= 0 && index < max_inner_slot_size,
                                       "If the entry can be found, its index should be in the appropriate range.");
                    }
                    i++;
                }
            }
            return index;
        }

        int find_inner_entry_index_id(const InnerNode *node, const key_type &key, const key_type &id, const version_type &version, bool filter = false)
        {
            int index = -1;
            int i = 0;

            if (USE_MAX_KEY)
            {
                while (i < node->used_slot_size)
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_contain_key_id(node->entries[i], key, id))
                    {
                        index = i;
                        return index;
                    }
                    i++;
                }
            }
            else
            {
                while (i < node->used_slot_size)
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_potentially_contain_key(node->entries[i], key))
                    {
                        if (index == -1)
                        {
                            index = i;
                        }
                        else if (key_greater(node->entries[i].key, node->entries[index].key))
                        {
                            index = i;
                        }
                        MVBTREE_ASSERT(index >= 0 && index < max_inner_slot_size,
                                       "If the entry can be found, its index should be in the appropriate range.");
                    }
                    i++;
                }
            }
            return index;
        }
        std::vector<int> find_inner_entry_indexes(const InnerNode *node, const KeyRange &keyrange, const version_type &version, bool filter = false)
        {
            std::vector<int> v_index;
            std::vector<int> v_potential_index;
            int i = 0;

            if (USE_MAX_KEY)
            {
                while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, version))
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_overlap_keyrange(node->entries[i], keyrange))
                    {
                        v_index.push_back(i);
                    }
                    i++;
                }
            }
            else
            {
                while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, version))
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_contain_version(node->entries[i], version) && inner_entry_potentially_contain_key(node->entries[i], keyrange.max_key))
                    {
                        v_potential_index.push_back(i);
                    }
                    i++;
                }

                if (v_potential_index.empty())
                    return v_index;

                for (size_t k = 0; k < v_potential_index.size() - 1; k++)
                {
                    if (!inner_entry_potentially_contain_key(node->entries[v_potential_index[k]], keyrange.min_key))
                    {
                        v_index.push_back(v_potential_index[k]);
                    }
                    else if (!inner_entry_potentially_contain_key(node->entries[v_potential_index[k] + 1], keyrange.min_key))
                    {
                        v_index.push_back(v_potential_index[k]);
                    }
                    else if (version_less(node->entries[v_potential_index[k]].lifespan.start_version, node->entries[v_potential_index[k] + 1].lifespan.start_version))
                    {
                        v_index.push_back(v_potential_index[k]);
                    }
                }
                v_index.push_back(v_potential_index[v_potential_index.size() - 1]);
            }

            return v_index;
        }

        std::vector<int> find_inner_entry_indexes(const InnerNode *node, const key_type &key, const Lifespan &lifespan, bool filter = false)
        {
            std::vector<int> v_index;
            std::vector<int> v_potential_index;
            int i = 0;

            if (USE_MAX_KEY)
            {
                while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, lifespan.end_version))
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_overlap_lifespan(node->entries[i], lifespan) && inner_entry_contain_key(node->entries[i], key))
                    {
                        v_index.push_back(i);
                    }
                    i++;
                }
            }
            else
            {
            }

            return v_index;
        }

        std::vector<int> find_inner_entry_indexes(const InnerNode *node, const KeyRange &keyrange, const Lifespan &lifespan, bool filter = false)
        {

            std::vector<int> v_index;
            std::vector<int> v_potential_index;
            int i = 0;

            if (USE_MAX_KEY)
            {
                while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, lifespan.end_version))
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_overlap_lifespan(node->entries[i], lifespan) && inner_entry_overlap_keyrange(node->entries[i], keyrange))
                    {
                        v_index.push_back(i);
                    }
                    i++;
                }
            }
            else
            {
                while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, lifespan.end_version))
                {
                    if ((!filter || (filter && node->entries[i].entry_type)) && inner_entry_overlap_lifespan(node->entries[i], lifespan) && inner_entry_potentially_contain_key(node->entries[i], keyrange.max_key))
                    {
                        v_potential_index.push_back(i);
                    }
                    i++;
                }

                if (v_potential_index.empty())
                    return v_index;

                for (size_t k = 0; k < v_potential_index.size() - 1; k++)
                {
                    if (!inner_entry_potentially_contain_key(node->entries[v_potential_index[k]], keyrange.min_key))
                    {
                        v_index.push_back(v_potential_index[k]);
                    }
                    else if (!inner_entry_potentially_contain_key(node->entries[v_potential_index[k] + 1], keyrange.min_key))
                    {

                        v_index.push_back(v_potential_index[k]);
                    }
                    else if (version_less(node->entries[v_potential_index[k]].lifespan.start_version, node->entries[v_potential_index[k] + 1].lifespan.start_version))
                    {

                        v_index.push_back(v_potential_index[k]);
                    }
                }
                v_index.push_back(v_potential_index[v_potential_index.size() - 1]);
            }

            return v_index;
        }

        int find_alive_leaf_entry_index(const LeafNode *node, const key_type &key)
        {
            for (int i = 0; i < node->used_slot_size; i++)
            {
                if (node->entries[i].is_alive() && leaf_entry_contain_key(node->entries[i], key))
                {
                    return i;
                }
            }

            return -1;
        }

        int find_alive_leaf_entry_index_id(const LeafNode *node, const key_type &key, const key_type &id)
        {
            for (int i = 0; i < node->used_slot_size; i++)
            {
                if (node->entries[i].is_alive() && leaf_entry_contain_id(node->entries[i], id))
                {
                    return i;
                }
            }

            return -1;
        }

        void find_leaf_entry(const LeafNode *node, const key_type &key, const version_type &version, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            int i = 0;


            while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, version))
            {
                if ((!filter || (filter && node->entries[i].entry_type)) && leaf_entry_contain_version(node->entries[i], version) && leaf_entry_contain_key(node->entries[i], key))
                {
                    v_res.push_back(node->entries[i]);
                }
                i++;
            }
        }

        void find_leaf_entry(const LeafNode *node, const KeyRange &keyrange, const version_type &version, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            int i = 0;

            while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, version))
            {
                if ((!filter || (filter && node->entries[i].entry_type)) && leaf_entry_contain_version(node->entries[i], version) && leaf_entry_overlap_keyrange(node->entries[i], keyrange))
                {
                    v_res.push_back(node->entries[i]);
                }
                i++;
            }
        }

        void find_leaf_entry(const LeafNode *node, const key_type &key, const Lifespan &lifespan, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            int i = 0;

            while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, lifespan.end_version))
            {
                if ((!filter || (filter && node->entries[i].entry_type)) && leaf_entry_overlap_lifespan(node->entries[i], lifespan) && leaf_entry_contain_key(node->entries[i], key))
                {
                    v_res.push_back(node->entries[i]);
                }
                i++;
            }
        }

        size_t find_leaf_entry(const LeafNode *node, const KeyRange &keyrange, const Lifespan &lifespan, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            int i = 0;
            size_t result = 0;
            while (i < node->used_slot_size && version_lessequal(node->entries[i].lifespan.start_version, lifespan.end_version))
            {
                if ((!filter || (filter && node->entries[i].entry_type)) && leaf_entry_overlap_lifespan(node->entries[i], lifespan) && leaf_entry_overlap_keyrange(node->entries[i], keyrange))
                {
#ifdef WORKLOAD_COUNT
                            result++;
#else
                            result ^= (node->entries[i].key);
#endif
                }
                i++;
            }

            return result;
        }

    private:

        std::stack<std::pair<int, Node *>> get_path_from_root_to_leaf(const key_type &key)
        {
            std::stack<std::pair<int, Node *>> path;

            if (!m_root)
                return path;
            Node *node = m_root;

            while (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                int slot = find_inner_entry_index(inner, key, m_current_version);
                node = inner->entries[slot].ptr_child;

                std::pair<int, Node *> pair;
                pair.first = slot;  
                pair.second = node; 
                path.push(pair);
            }

            return path;
        }

        std::stack<std::pair<int, Node *>> get_path_from_root_to_leaf_id(const key_type &key, const key_type &id)
        {
            std::stack<std::pair<int, Node *>> path;

            if (!m_root)
                return path;
            Node *node = m_root;

            while (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                int slot = find_inner_entry_index_id(inner, key, id, m_current_version);
                node = inner->entries[slot].ptr_child;

                std::pair<int, Node *> pair;
                pair.first = slot;  
                pair.second = node; 
                path.push(pair);
            }

            return path;
        }
        
        int find_sibling_slot_to_merge(InnerNode *parent_node, int slot_in_parent_node)
        {
            Node *merged_node = parent_node->entries[slot_in_parent_node].ptr_child;

            if (USE_MAX_KEY)
            {
                for (int i = 0; i < parent_node->used_slot_size; i++)
                {
                    Node *candidate = parent_node->entries[i].ptr_child;
                    if (candidate->is_alive() &&
                        (candidate->keyrange.min_key == merged_node->keyrange.max_key ||
                         candidate->keyrange.max_key == merged_node->keyrange.min_key))
                    {
                        MVBTREE_ASSERT(i != slot_in_parent_node, "The sibling cannot be itself.");
                        return i;
                    }
                }
                MVBTREE_PRINT("The sibling cannot be found.");
                
                return -1;
            }
            else
            {
                int sibling_slot = 0;
                while (!parent_node->entries[sibling_slot].ptr_child->is_alive())
                {
                    sibling_slot++;
                }
                Node *first_candidate = parent_node->entries[sibling_slot].ptr_child;
                key_type min_key_distance = (first_candidate->keyrange.min_key > merged_node->keyrange.min_key) ? (first_candidate->keyrange.min_key - merged_node->keyrange.min_key) : (merged_node->keyrange.min_key - first_candidate->keyrange.min_key);

                for (int i = sibling_slot + 1; i < parent_node->used_slot_size; i++)
                {
                    Node *candidate = parent_node->entries[i].ptr_child;
                    key_type current_key_distance = (candidate->keyrange.min_key > merged_node->keyrange.min_key) ? (candidate->keyrange.min_key - merged_node->keyrange.min_key) : (merged_node->keyrange.min_key - candidate->keyrange.min_key);

                    if (candidate->is_alive() && i != slot_in_parent_node &&
                        key_less(current_key_distance, min_key_distance))
                    {
                        sibling_slot = i;
                        min_key_distance = current_key_distance;
                    }
                }

                return sibling_slot;
            }
        }

#pragma endregion

#pragma region query
    public:
        std::vector<LeafEntry> query_key_timestamp(const key_type &key, const version_type &version)
        {
            return query_key_timestamp_start(key, version);
        }

        std::vector<LeafEntry> query_range_timestamp(const key_type &min_key, const key_type &max_key, const version_type &version)
        {
            return query_range_timestamp_start(KeyRange(min_key, max_key), version);
        }

        std::vector<LeafEntry> query_range_lifespan(const key_type &min_key, const key_type &max_key, const version_type &start_version, const version_type &end_version)
        {
            return query_range_lifespan_start(KeyRange(min_key, max_key), Lifespan(start_version, end_version));
        }

        size_t execute_rangeTimeTravel(const key_type &min_key, const key_type &max_key, const version_type &start_version, const version_type &end_version)
        {
            return query_range_lifespan_cardi_start(KeyRange(min_key, max_key), Lifespan(start_version, end_version));
        }

    private:
        Node *get_root(const version_type &version)
        {
            Node *node;
            MVBTREE_ASSERT(version_lessequal(version, m_current_version), "The query version has not heppened yet.");

            if (version_lessequal(m_rootbox.lifespan.start_version, version))
            {
                node = m_root;
            }
            else
            {

                auto box = m_roots.get_qualified_leaf_latest(version);
                node = box.ptr_root;
            }
            return node;
        }

        void query_once(Node *node, const key_type &key, const version_type &version, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            while (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                int slot = find_inner_entry_index(inner, key, version, filter);
                node = inner->entries[slot].ptr_child;
            }

            const LeafNode *leaf = static_cast<const LeafNode *>(node);
            find_leaf_entry(leaf, key, version, v_res, filter);
        }

        void query_recursion(Node *node, const KeyRange &keyrange, const version_type &version, std::vector<LeafEntry> &v_res, bool filter = false)
        {
            if (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                std::vector<int> v_slot = find_inner_entry_indexes(inner, keyrange, version, filter);

                for (std::vector<int>::iterator it = v_slot.begin(); it != v_slot.end(); ++it)
                {
                    Node *child = inner->entries[*it].ptr_child;
                    query_recursion(child, keyrange, version, v_res, filter);
                }
            }
            else
            {
                const LeafNode *leaf = static_cast<const LeafNode *>(node);
                find_leaf_entry(leaf, keyrange, version, v_res, filter);
            }
        }

        void query_recursion_key_lifespan(Node *node, const key_type &key, const Lifespan &lifespan,
                                          std::vector<LeafEntry> &v_res, bool filter = false)
        {
            if (PRINT_QUERY_NODES)
                node->print();

            if (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                std::vector<int> v_slot = find_inner_entry_indexes(inner, key, lifespan, filter);

                if (!v_slot.empty())
                {
                    std::vector<int>::iterator it = v_slot.begin();
                    query_recursion_key_lifespan(inner->entries[*it].ptr_child, key, lifespan, v_res, filter);
                    ++it;

                    while (it != v_slot.end())
                    {
                        Node *child = inner->entries[*it].ptr_child;
                        
                        query_recursion_key_lifespan(child, key, lifespan, v_res, filter);
                        ++it;
                    }
                }
            }
            else
            {
                const LeafNode *leaf = static_cast<const LeafNode *>(node);
                find_leaf_entry(leaf, key, lifespan, v_res, filter);

            }
        }

        size_t query_recursion_both(Node *node, const KeyRange &keyrange, const Lifespan &lifespan,
                                  std::vector<LeafEntry> &v_res, bool filter = false)
        {
            size_t result =0;
            if (PRINT_QUERY_NODES)
                node->print();

            if (!node->is_leaf())
            {
                const InnerNode *inner = static_cast<const InnerNode *>(node);
                std::vector<int> v_slot = find_inner_entry_indexes(inner, keyrange, lifespan, filter);

                if (!v_slot.empty())
                {
                    std::vector<int>::iterator it = v_slot.begin();
#ifdef WORKLOAD_COUNT
                            result += query_recursion_both(inner->entries[*it].ptr_child, keyrange, lifespan, v_res, filter);
#else
                            result ^= query_recursion_both(inner->entries[*it].ptr_child, keyrange, lifespan, v_res, filter);
#endif
                    ++it;

                    while (it != v_slot.end())
                    {
                        Node *child = inner->entries[*it].ptr_child;
#ifdef WORKLOAD_COUNT
                        result += query_recursion_both(child, keyrange, lifespan, v_res, filter);
#else
                        result ^= query_recursion_both(child, keyrange, lifespan, v_res, filter);
#endif
                        ++it;
                    }
                }
            }
            else
            {
                const LeafNode *leaf = static_cast<const LeafNode *>(node);
#ifdef WORKLOAD_COUNT
                        result += find_leaf_entry(leaf, keyrange, lifespan, v_res, filter);
#else
                        result ^= find_leaf_entry(leaf, keyrange, lifespan, v_res, filter);
#endif
            
            }
            return result;
        }

        int get_cardinality(std::vector<LeafEntry> &v_entries)
        {
            int cardinality = 0;
            
            if (!COPY_ENTRY_WITH_SEGMENT)
            {

                std::map<key_type, size_t> p_key_slot = {};
                std::vector<LeafEntry> v_old = v_entries;
                size_t old_size = v_old.size();

                for (size_t i = 0; i < old_size; i++)
                {
                    if (p_key_slot.count(v_old[i].key) == 0)
                    {
                        p_key_slot.emplace(v_old[i].key, i);
                        cardinality++;
                    }
                }
            }
            else
            {
                
            }
            return cardinality;
        }

    private:

        std::vector<LeafEntry> query_key_timestamp_start(const key_type &key, const version_type &version)
        {
            std::vector<LeafEntry> v_res;
            if (!m_root)
                return v_res;

            Node *node = get_root(version);
            query_once(node, key, version, v_res);

            return v_res;
        }

    private:

        std::vector<LeafEntry> query_range_timestamp_start(const KeyRange &keyrange, const version_type &version)
        {
            std::vector<LeafEntry> v_res;
            if (!m_root)
                return v_res;

            Node *node = get_root(version);
            query_recursion(node, keyrange, version, v_res);

            return v_res;
        }

    private:

        size_t query_range_lifespan_start(const KeyRange &keyrange, const Lifespan &lifespan)
        {
            std::vector<LeafEntry> v_res;
            size_t result = 0;

            if (!m_root)
                return 0;

            Node *searched_root = get_root(lifespan.start_version);
#ifdef WORKLOAD_COUNT
                            result += query_recursion_both(searched_root, keyrange, lifespan, v_res, false);
#else
                            result ^= query_recursion_both(searched_root, keyrange, lifespan, v_res, false);
#endif

            while (!searched_root->is_alive() && searched_root->lifespan.end_version <= lifespan.end_version)
            {
                searched_root = get_root(searched_root->lifespan.end_version);
                if (COPY_ENTRY_WITH_SEGMENT)
                {
#ifdef WORKLOAD_COUNT
                            result += query_recursion_both(searched_root, keyrange, lifespan, v_res, false);
#else
                            result ^= query_recursion_both(searched_root, keyrange, lifespan, v_res, false);
#endif
                }
                else 
                if (COPY_ENTRY_WITH_SEGMENT)
                {
#ifdef WORKLOAD_COUNT
                            result += query_recursion_both(searched_root, keyrange, lifespan, v_res, true);
#else
                            result ^= query_recursion_both(searched_root, keyrange, lifespan, v_res, true);
#endif
                }
            }

            return result;
        }

        size_t query_range_lifespan_cardi_start(const KeyRange &keyrange, const Lifespan &lifespan)
        {
            size_t result = query_range_lifespan_start(keyrange, lifespan);

            return result;
        }

#pragma endregion

#pragma region public insert
    public:

        void insert(const version_type &version, const pair_type &x)
        {
            insert_start(version, x.first, x.second);
        }

        void insert(const version_type &version, const key_type &key, const key_type &id)
        {
            insert_start(version, key, id);
        }

    private:

        void insert_start(const version_type &version, const key_type &key, const key_type &id)
        {
            set_m_current_version(version);

            if (m_root == NULL)
            {
                m_root = allocate_leaf_node(KeyRange(m_min_key), m_current_version);
                m_rootbox = RootBox(KeyRange(m_min_key, m_min_key), m_current_version, m_root);
            }

            std::stack<std::pair<int, Node *>> path = get_path_from_root_to_leaf(key);

            LeafEntry new_leaf_entry(key, version, id);

            bool insert_result = leaf_block_insert(new_leaf_entry, path);

            if (insert_result)
                m_stats.item_size++;
        }

        Node *get_node_from_path(std::stack<std::pair<int, Node *>> &path, bool &is_root, int &slot_in_parent_node)
        {
            Node *node;
            is_root = path.empty();

            if (is_root)
            {
                node = m_root;
            }
            else
            {
                slot_in_parent_node = path.top().first;
                node = path.top().second;
                path.pop();
            }

            return node;
        }

        bool leaf_block_insert(const LeafEntry &leaf_entry, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(node->is_leaf(), "The expected inserted node should be a leaf node.");
            LeafNode *leaf_node = static_cast<LeafNode *>(node);

            if (leaf_node->is_block_overflow(1) && !is_root)
            {
                Node *parent = (path.empty()) ? m_root : path.top().second;
                InnerNode *parent_node = static_cast<InnerNode *>(parent);
                treat_nonroot_leaf_block_overflow(leaf_node, leaf_entry, parent_node, slot_in_parent_node, path);
            }
            else if (leaf_node->is_block_overflow(1) && is_root)
            {
                treat_root_leaf_block_overflow(leaf_node, leaf_entry);
            }
            else
            {
                leaf_node->insert_entry(leaf_entry);
            }

            update_m_rootbox(leaf_entry.key);

            return true;
        }

        void treat_nonroot_leaf_block_overflow(LeafNode *leaf_node, const LeafEntry &leaf_entry, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(leaf_node->is_leaf(), "The node should be a leaf node.");
            MVBTREE_ASSERT(leaf_node->is_block_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            LeafNode *new_node = version_split_leaf_node(leaf_node);

            if (new_node->is_strong_version_overflow(1))
            {
                LeafNode *second_new_node = key_split(new_node, leaf_entry);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_one(new_parent_entry, second_new_parent_entry, slot_in_parent_node, path);
            }
            else if (new_node->is_strong_version_underflow(1))
            {
                int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
                LeafNode *sibling_leaf = static_cast<LeafNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
                bool sibling_key_is_smaller = key_less(sibling_leaf->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

                std::vector<LeafEntry> v_extra_entries;
                v_extra_entries.push_back(leaf_entry);

                if (!new_node->is_strong_version_overflow(sibling_leaf->alive_slot_size))
                {
                    merge_leaf(new_node, sibling_leaf, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child = static_cast<Node *>(new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                    insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
                else
                {
                    LeafNode *second_new_node = key_split_after_merge_leaf(new_node, sibling_leaf, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child_first = static_cast<Node *>(new_node);
                    Node *ptr_child_second = static_cast<Node *>(second_new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                    InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                    insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
            }
            else
            {
                new_node->insert_entry(leaf_entry);

                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_one(new_parent_entry, slot_in_parent_node, path);
            }
        }

        void treat_root_leaf_block_overflow(LeafNode *leaf_node, const LeafEntry &leaf_entry)
        {
            MVBTREE_ASSERT(leaf_node->is_leaf(), "The node should be a leaf node.");
            MVBTREE_ASSERT(leaf_node->is_block_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            LeafNode *new_node = version_split_leaf_node(leaf_node);

            m_rootbox.end(m_current_version);
            m_roots.insert(m_rootbox.lifespan.start_version, m_rootbox);

            if (new_node->is_strong_version_overflow(1))
            {
                KeyRange maintained_keyrange = new_node->keyrange;
                LeafNode *second_new_node = key_split(new_node, leaf_entry);

                InnerNode *new_root = allocate_inner_node(1, maintained_keyrange, new_node->lifespan.start_version);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                new_root->insert_entry(new_parent_entry);
                new_root->insert_entry(second_new_parent_entry);
                m_root = new_root;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
            else
            {
                new_node->insert_entry(leaf_entry);
                m_root = new_node;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
        }

        void treat_nonroot_inner_block_overflow(InnerNode *inner_node, const InnerEntry &inner_entry, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_block_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            InnerNode *new_node = version_split_inner_node(inner_node);

            if (new_node->is_strong_version_overflow(1))
            {
                InnerNode *second_new_node = key_split(new_node, inner_entry);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_one(new_parent_entry, second_new_parent_entry, slot_in_parent_node, path);
            }
            else if (new_node->is_strong_version_underflow(1))
            {
                int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
                InnerNode *sibling_inner = static_cast<InnerNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
                bool sibling_key_is_smaller = key_less(sibling_inner->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

                std::vector<InnerEntry> v_extra_entries;
                v_extra_entries.push_back(inner_entry);

                if (!new_node->is_strong_version_overflow(sibling_inner->alive_slot_size))
                {
                    merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child = static_cast<Node *>(new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                    insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
                else
                {
                    InnerNode *second_new_node = key_split_after_merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child_first = static_cast<Node *>(new_node);
                    Node *ptr_child_second = static_cast<Node *>(second_new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                    InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                    insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
            }
            else
            {
                new_node->insert_entry(inner_entry);
                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_one(new_parent_entry, slot_in_parent_node, path);
            }
        }

        void treat_nonroot_inner_block_overflow_two(InnerNode *inner_node, const InnerEntry &inner_entry, const InnerEntry &second_entry, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_block_overflow(2), "Inserting 2 more entry would cause the node to block overflow.");

            InnerNode *new_node = version_split_inner_node(inner_node);

            if (new_node->is_strong_version_overflow(2))
            {
                InnerNode *second_new_node = key_split_two(new_node, inner_entry, second_entry);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_one(new_parent_entry, second_new_parent_entry, slot_in_parent_node, path);
            }
            else if (new_node->is_strong_version_underflow(2))
            {
                int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
                InnerNode *sibling_inner = static_cast<InnerNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
                bool sibling_key_is_smaller = key_less(sibling_inner->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

                std::vector<InnerEntry> v_extra_entries;
                v_extra_entries.push_back(inner_entry);
                v_extra_entries.push_back(second_entry);

                if (!new_node->is_strong_version_overflow(sibling_inner->alive_slot_size))
                {
                    merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child = static_cast<Node *>(new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                    insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
                else
                {
                    InnerNode *second_new_node = key_split_after_merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                    Node *ptr_child_first = static_cast<Node *>(new_node);
                    Node *ptr_child_second = static_cast<Node *>(second_new_node);
                    InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                    InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                    insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
                }
            }
            else
            {
                new_node->insert_entry(inner_entry);
                new_node->insert_entry(second_entry);

                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_one(new_parent_entry, slot_in_parent_node, path);
            }
        }

        void treat_root_inner_block_overflow(InnerNode *inner_node, const InnerEntry &inner_entry)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_block_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            InnerNode *new_node = version_split_inner_node(inner_node);

            m_rootbox.end(m_current_version);
            m_roots.insert(m_rootbox.lifespan.start_version, m_rootbox);

            if (new_node->is_strong_version_overflow(1))
            {
                KeyRange maintained_keyrange = new_node->keyrange;
                InnerNode *second_new_node = key_split(new_node, inner_entry);

                InnerNode *new_root = allocate_inner_node(new_node->level + 1, maintained_keyrange, new_node->lifespan.start_version);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                new_root->insert_entry(new_parent_entry);
                new_root->insert_entry(second_new_parent_entry);
                m_root = new_root;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
            else
            {
                new_node->insert_entry(inner_entry);
                m_root = new_node;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
        }

        void treat_root_inner_block_overflow_two(InnerNode *inner_node, const InnerEntry &inner_entry, const InnerEntry &second_entry)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_block_overflow(2), "Inserting 1 more entry would cause the node to block overflow.");

            InnerNode *new_node = version_split_inner_node(inner_node);

            m_rootbox.end(m_current_version);
            m_roots.insert(m_rootbox.lifespan.start_version, m_rootbox);

            if (new_node->is_strong_version_overflow(2))
            {
                KeyRange maintained_keyrange = new_node->keyrange;
                InnerNode *second_new_node = key_split_two(new_node, inner_entry, second_entry);

                InnerNode *new_root = allocate_inner_node(new_node->level + 1, maintained_keyrange, new_node->lifespan.start_version);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                new_root->insert_entry(new_parent_entry);
                new_root->insert_entry(second_new_parent_entry);
                m_root = new_root;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
            else
            {
                new_node->insert_entry(inner_entry);
                new_node->insert_entry(second_entry);
                m_root = new_node;
                m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
            }
        }

        void copy_alive_leaf_entries_into_vector(LeafNode *node, std::vector<LeafEntry> &v_entries, bool enable_sort = true)
        {
            for (int i = 0; i < node->used_slot_size; i++)
            {
                if (node->entries[i].is_alive())
                {
                    LeafEntry new_entry = node->entries[i];
                    new_entry.set_entry_type(false);
                    if (COPY_ENTRY_WITH_SEGMENT)
                    {
                        node->entries[i].end(m_current_version);
                        new_entry.lifespan.set_start_version(m_current_version);
                    }
                    v_entries.push_back(new_entry);
                }
            }

            if (enable_sort)
            {
                if (COPY_ENTRY_WITH_SEGMENT)
                {
                    sort_vector_by_key(v_entries);
                }
                else
                {
                    sort_vector_by_version(v_entries);
                }
            }
        }

        void copy_alive_inner_entries_into_vector(InnerNode *node, std::vector<InnerEntry> &v_entries, bool enable_sort = true)
        {
            for (int i = 0; i < node->used_slot_size; i++)
            {
                if (node->entries[i].is_alive())
                {
                    InnerEntry new_entry = node->entries[i];
                    new_entry.set_entry_type(false);
                    if (COPY_ENTRY_WITH_SEGMENT)
                    {
                        node->entries[i].end(m_current_version);
                        new_entry.lifespan.set_start_version(m_current_version);
                    }
                    v_entries.push_back(new_entry);
                }
            }

            if (enable_sort)
            {
                if (COPY_ENTRY_WITH_SEGMENT)
                {
                    sort_vector_by_key(v_entries);
                }
                else
                {
                    sort_vector_by_version(v_entries);
                }
            }
        }


        LeafNode *version_split_leaf_node(LeafNode *node)
        {
            MVBTREE_ASSERT(node->is_leaf(), "The node should be a leaf node.");

            node->end_node(m_current_version);
            m_stats.alive_leaf_node_size--;

            LeafNode *new_node = allocate_leaf_node(node->keyrange, m_current_version);
            std::vector<LeafEntry> v_entries;
            copy_alive_leaf_entries_into_vector(node, v_entries);
            new_node->fill_entries(v_entries);

            return new_node;
        }

        InnerNode *version_split_inner_node(InnerNode *node)
        {
            MVBTREE_ASSERT(!node->is_leaf(), "The node should be an inner node.");

            node->end_node(m_current_version);
            m_stats.alive_inner_node_size--;

            InnerNode *new_node = allocate_inner_node(node->level, node->keyrange, m_current_version);
            std::vector<InnerEntry> v_entries;
            copy_alive_inner_entries_into_vector(node, v_entries);
            new_node->fill_entries(v_entries);

            return new_node;
        }

        void merge_leaf(LeafNode *node, LeafNode *sibling, const bool sibling_key_is_smaller, const std::vector<LeafEntry> v_extra_entries)
        {
            sibling->end_node(m_current_version);
            m_stats.alive_leaf_node_size--;

            MVBTREE_ASSERT(node->alive_slot_size == node->used_slot_size, "All entries of the node to be merged should be alive.");
            std::vector<LeafEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            for (size_type i = 0; i < v_extra_entries.size(); i++)
            {
                v_entries.push_back(v_extra_entries[i]);
            }
            copy_alive_leaf_entries_into_vector(sibling, v_entries);
            node->empty_entries();
            node->fill_entries(v_entries);

            if (sibling_key_is_smaller)
            {
                node->keyrange.min_key = sibling->keyrange.min_key;
            }
            else
            {
                node->keyrange.max_key = sibling->keyrange.max_key;
            }
        }

        void merge_inner(InnerNode *node, InnerNode *sibling, const bool sibling_key_is_smaller, const std::vector<InnerEntry> v_extra_entries)
        {
            sibling->end_node(m_current_version);
            m_stats.alive_inner_node_size--;

            MVBTREE_ASSERT(node->alive_slot_size == node->used_slot_size, "All entries of the node to be merged should be alive.");
            std::vector<InnerEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            for (size_type i = 0; i < v_extra_entries.size(); i++)
            {
                v_entries.push_back(v_extra_entries[i]);
            }
            copy_alive_inner_entries_into_vector(sibling, v_entries);
            node->empty_entries();
            node->fill_entries(v_entries);

            if (sibling_key_is_smaller)
            {
                node->keyrange.min_key = sibling->keyrange.min_key;
            }
            else
            {
                node->keyrange.max_key = sibling->keyrange.max_key;
            }
        }

        LeafNode *key_split(LeafNode *node, const LeafEntry &new_entry)
        {
            MVBTREE_ASSERT(node->is_leaf(), "The node should be a leaf node.");
            MVBTREE_ASSERT(node->is_strong_version_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            std::vector<LeafEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            v_entries.push_back(new_entry);
            sort_vector_by_key(v_entries);

            size_type const half_size = (v_entries.size() % 2 == 0) ? v_entries.size() / 2 : v_entries.size() / 2 + 1;
            key_type split_key = (v_entries.begin() + half_size)->key;

            std::vector<LeafEntry> v_entries_lo(v_entries.begin(), v_entries.begin() + half_size); // iterator ranges represent half open ranges [begin, end)
            std::vector<LeafEntry> v_entries_hi(v_entries.begin() + half_size, v_entries.end());   // iterator ranges represent half open ranges [begin, end)

            MVBTREE_ASSERT(split_key == v_entries_hi.front().key, "The splitting key should be the medium key.");

            if (COPY_ENTRY_WITH_SEGMENT)
            {
                sort_vector_by_version(v_entries_lo);
                sort_vector_by_version(v_entries_hi);
            }

            node->empty_entries();
            node->fill_entries(v_entries_lo);

            LeafNode *new_node = allocate_leaf_node(KeyRange(split_key, node->keyrange.max_key), m_current_version);
            new_node->fill_entries(v_entries_hi);

            node->keyrange.max_key = split_key;

            return new_node;
        }

        InnerNode *key_split(InnerNode *node, const InnerEntry &new_entry)
        {
            MVBTREE_ASSERT(!node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(node->is_strong_version_overflow(1), "Inserting 1 more entry would cause the node to block overflow.");

            std::vector<InnerEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            v_entries.push_back(new_entry);
            sort_vector_by_key(v_entries);

            size_type const half_size = (v_entries.size() % 2 == 0) ? v_entries.size() / 2 : v_entries.size() / 2 + 1;
            key_type split_key = (v_entries.begin() + half_size)->key;

            std::vector<InnerEntry> v_entries_lo(v_entries.begin(), v_entries.begin() + half_size); // iterator ranges represent half open ranges [begin, end)
            std::vector<InnerEntry> v_entries_hi(v_entries.begin() + half_size, v_entries.end());   // iterator ranges represent half open ranges [begin, end)

            MVBTREE_ASSERT(split_key == v_entries_hi.front().key, "The splitting key should be the medium key.");

            if (COPY_ENTRY_WITH_SEGMENT)
            {
                sort_vector_by_version(v_entries_lo);
                sort_vector_by_version(v_entries_hi);
            }

            node->empty_entries();
            node->fill_entries(v_entries_lo);

            InnerNode *new_node = allocate_inner_node(node->level, KeyRange(split_key, node->keyrange.max_key), m_current_version);
            new_node->fill_entries(v_entries_hi);

            node->keyrange.max_key = split_key;

            return new_node;
        }

        InnerNode *key_split_two(InnerNode *node, const InnerEntry &new_entry, const InnerEntry &second_entry)
        {
            MVBTREE_ASSERT(!node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(node->is_strong_version_overflow(2), "Inserting 2 more entries would cause the node to block overflow.");

            std::vector<InnerEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            v_entries.push_back(new_entry);
            v_entries.push_back(second_entry);
            sort_vector_by_key(v_entries);

            size_type const half_size = (v_entries.size() % 2 == 0) ? v_entries.size() / 2 : v_entries.size() / 2 + 1;
            key_type split_key = (v_entries.begin() + half_size)->key;

            std::vector<InnerEntry> v_entries_lo(v_entries.begin(), v_entries.begin() + half_size); 
            std::vector<InnerEntry> v_entries_hi(v_entries.begin() + half_size, v_entries.end());   

            MVBTREE_ASSERT(split_key == v_entries_hi.front().key, "The splitting key should be the medium key.");

            if (COPY_ENTRY_WITH_SEGMENT)
            {
                sort_vector_by_version(v_entries_lo);
                sort_vector_by_version(v_entries_hi);
            }

            node->empty_entries();
            node->fill_entries(v_entries_lo);

            InnerNode *new_node = allocate_inner_node(node->level, KeyRange(split_key, node->keyrange.max_key), m_current_version);
            new_node->fill_entries(v_entries_hi);

            node->keyrange.max_key = split_key;

            return new_node;
        }

        LeafNode *key_split_after_merge_leaf(LeafNode *node, LeafNode *sibling, const bool sibling_key_is_smaller, const std::vector<LeafEntry> v_extra_entries)
        {
            sibling->end_node(m_current_version);
            m_stats.alive_leaf_node_size--;

            MVBTREE_ASSERT(node->alive_slot_size == node->used_slot_size, "All entries of the node to be merged should be alive.");
            std::vector<LeafEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            for (size_type i = 0; i < v_extra_entries.size(); i++)
            {
                v_entries.push_back(v_extra_entries[i]);
            }
            copy_alive_leaf_entries_into_vector(sibling, v_entries, false);
            sort_vector_by_key(v_entries);

            if (sibling_key_is_smaller)
            {
                node->keyrange.min_key = sibling->keyrange.min_key;
            }
            else
            {
                node->keyrange.max_key = sibling->keyrange.max_key;
            }

            size_type const half_size = (v_entries.size() % 2 == 0) ? v_entries.size() / 2 : v_entries.size() / 2 + 1;
            key_type split_key = (v_entries.begin() + half_size)->key;

            std::vector<LeafEntry> v_entries_lo(v_entries.begin(), v_entries.begin() + half_size);
            std::vector<LeafEntry> v_entries_hi(v_entries.begin() + half_size, v_entries.end());

            MVBTREE_ASSERT(split_key == v_entries_hi.front().key, "The splitting key should be the medium key.");

            if (COPY_ENTRY_WITH_SEGMENT)
            {
                sort_vector_by_version(v_entries_lo);
                sort_vector_by_version(v_entries_hi);
            }

            node->empty_entries();
            node->fill_entries(v_entries_lo);

            LeafNode *node_hi = allocate_leaf_node(KeyRange(split_key, node->keyrange.max_key), m_current_version);
            node_hi->fill_entries(v_entries_hi);

            node->keyrange.max_key = split_key;

            return node_hi;
        }

        InnerNode *key_split_after_merge_inner(InnerNode *node, InnerNode *sibling, const bool sibling_key_is_smaller, const std::vector<InnerEntry> v_extra_entries)
        {
            sibling->end_node(m_current_version);
            m_stats.alive_inner_node_size--;

            MVBTREE_ASSERT(node->alive_slot_size == node->used_slot_size, "All entries of the node to be merged should be alive.");
            std::vector<InnerEntry> v_entries(node->entries, node->entries + node->used_slot_size);
            for (size_type i = 0; i < v_extra_entries.size(); i++)
            {
                v_entries.push_back(v_extra_entries[i]);
            }
            copy_alive_inner_entries_into_vector(sibling, v_entries, false);
            sort_vector_by_key(v_entries);

            if (sibling_key_is_smaller)
            {
                node->keyrange.min_key = sibling->keyrange.min_key;
            }
            else
            {
                node->keyrange.max_key = sibling->keyrange.max_key;
            }

            size_type const half_size = (v_entries.size() % 2 == 0) ? v_entries.size() / 2 : v_entries.size() / 2 + 1;
            key_type split_key = (v_entries.begin() + half_size)->key;

            std::vector<InnerEntry> v_entries_lo(v_entries.begin(), v_entries.begin() + half_size); 
            std::vector<InnerEntry> v_entries_hi(v_entries.begin() + half_size, v_entries.end());   

            MVBTREE_ASSERT(split_key == v_entries_hi.front().key, "The splitting key should be the medium key.");

            if (COPY_ENTRY_WITH_SEGMENT)
            {
                sort_vector_by_version(v_entries_lo);
                sort_vector_by_version(v_entries_hi);
            }

            node->empty_entries();
            node->fill_entries(v_entries_lo);

            InnerNode *node_hi = allocate_inner_node(node->level, KeyRange(split_key, node->keyrange.max_key), m_current_version);
            node_hi->fill_entries(v_entries_hi);

            node->keyrange.max_key = split_key;

            return node_hi;
        }

#pragma endregion

#pragma region public delete
    public:

        void erase(const version_type &version, const key_type &key)
        {
            delete_start(version, key);
        }

        void erase_id(const version_type &version, const key_type &key, const key_type &id)
        {
            delete_start_id(version, key, id);
        }

    private:

        void delete_start(const version_type &version, const key_type &key)
        {
            set_m_current_version(version);

            MVBTREE_ASSERT(m_root, "No deletion is allowed to an empty tree.")

            std::stack<std::pair<int, Node *>> path = get_path_from_root_to_leaf(key);
            bool delete_result = leaf_block_delete(key, path);

            if (delete_result)
            {

            }
            else
            {
                MVBTREE_PRINT("Cannot find such an alive entry to delete key " << key << " at version " << version);
            }
        }

        void delete_start_id(const version_type &version, const key_type &key, const key_type &id)
        {
            // MVBTREE_PRINT("delete_start "<< key <<" at version " << version);
            set_m_current_version(version);

            MVBTREE_ASSERT(m_root, "No deletion is allowed to an empty tree.")

            std::stack<std::pair<int, Node *>> path = get_path_from_root_to_leaf_id(key, id);
            bool delete_result = leaf_block_delete_id(key, id, path);

            if (delete_result)
            {

            }
            else
            {
                MVBTREE_PRINT("Cannot find such an alive entry to delete id " << id << " at version " << version);
            }
        }

        bool leaf_block_delete(const key_type &key, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(node->is_leaf(), "The expected deleted entry should be a leaf entry.");
            LeafNode *leaf_node = static_cast<LeafNode *>(node);
            int slot_in_node = find_alive_leaf_entry_index(leaf_node, key);

            if (slot_in_node == -1)
            {
                MVBTREE_PRINT("Cannot find such an alive entry to delete key " << key << " at version " << m_current_version);
                return false;
            }

            if (leaf_node->is_weak_version_underflow(-1) && !is_root)
            {
                Node *parent = (path.empty()) ? m_root : path.top().second;
                InnerNode *parent_node = static_cast<InnerNode *>(parent);
                treat_nonroot_leaf_block_underflow(leaf_node, slot_in_node, parent_node, slot_in_parent_node, path);
            }
            else if (leaf_node->is_weak_version_underflow(-1) && is_root)
            {
                treat_root_leaf_block_underflow(leaf_node, slot_in_node);
            }
            else
            {
                leaf_node->end_entry(slot_in_node, m_current_version);
            }

            return true;
        }

        bool leaf_block_delete_id(const key_type &key, const key_type &id, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(node->is_leaf(), "The expected deleted entry should be a leaf entry.");
            LeafNode *leaf_node = static_cast<LeafNode *>(node);
            int slot_in_node = find_alive_leaf_entry_index_id(leaf_node, key, id);

            if (slot_in_node == -1)
            {
                MVBTREE_PRINT("Cannot find such an alive entry to delete id " << id << " at version " << m_current_version);
                return false;
            }

            if (leaf_node->is_weak_version_underflow(-1) && !is_root)
            {
                Node *parent = (path.empty()) ? m_root : path.top().second;
                InnerNode *parent_node = static_cast<InnerNode *>(parent);
                treat_nonroot_leaf_block_underflow(leaf_node, slot_in_node, parent_node, slot_in_parent_node, path);
            }
            else if (leaf_node->is_weak_version_underflow(-1) && is_root)
            {
                treat_root_leaf_block_underflow(leaf_node, slot_in_node);
            }
            else
            {
                leaf_node->end_entry(slot_in_node, m_current_version);
            }

            return true;
        }
        void treat_nonroot_leaf_block_underflow(LeafNode *leaf_node, const int slot_in_node, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(leaf_node->is_leaf(), "The node should be a leaf node.");
            MVBTREE_ASSERT(leaf_node->is_weak_version_underflow(-1), "Deleting 1 more entry would cause the node to weak version underflow.");

            leaf_node->end_entry(slot_in_node, m_current_version);
            LeafNode *new_node = version_split_leaf_node(leaf_node);

            int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
            LeafNode *sibling_leaf = static_cast<LeafNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
            bool sibling_key_is_smaller = key_less(sibling_leaf->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

            std::vector<LeafEntry> v_extra_entries;

            if (!new_node->is_strong_version_overflow(sibling_leaf->alive_slot_size))
            {
                merge_leaf(new_node, sibling_leaf, sibling_key_is_smaller, v_extra_entries); 

                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
            else
            {
                LeafNode *second_new_node = key_split_after_merge_leaf(new_node, sibling_leaf, sibling_key_is_smaller, v_extra_entries);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
        }

        void treat_root_leaf_block_underflow(LeafNode *leaf_node, const int slot_in_node)
        {
            MVBTREE_ASSERT(leaf_node->is_leaf(), "The node should be a leaf node.");
            MVBTREE_ASSERT(leaf_node->is_weak_version_underflow(-1), "Deleting 1 more entry would cause the node to weak version underflow.");

            leaf_node->end_entry(slot_in_node, m_current_version);
            LeafNode *new_node = version_split_leaf_node(leaf_node);

            

            m_rootbox.end(m_current_version);
            m_roots.insert(m_rootbox.lifespan.start_version, m_rootbox);
            m_root = new_node;
            m_rootbox = RootBox(m_rootbox.keyrange, m_current_version, m_root);
        }

        void treat_nonroot_inner_block_underflow(InnerNode *inner_node, const int slot_in_node, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_weak_version_underflow(-1), "Deleting 1 more entry would cause the node to weak version underflow.");

            inner_node->end_entry(slot_in_node, m_current_version);
            InnerNode *new_node = version_split_inner_node(inner_node);

            int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
            InnerNode *sibling_inner = static_cast<InnerNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
            bool sibling_key_is_smaller = key_less(sibling_inner->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

            std::vector<InnerEntry> v_extra_entries;

            if (!new_node->is_strong_version_overflow(sibling_inner->alive_slot_size))
            {
                merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
            else
            {
                InnerNode *second_new_node = key_split_after_merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
        }

        void treat_nonroot_inner_block_underflow_two(InnerNode *inner_node, const int slot_in_node, const int slot_of_sibling_in_node, InnerNode *parent_node, int slot_in_parent_node, std::stack<std::pair<int, Node *>> &path)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_weak_version_underflow(-2), "Deleting 2 more entry would cause the node to weak version underflow.");

            inner_node->end_entry(slot_in_node, m_current_version);
            inner_node->end_entry(slot_of_sibling_in_node, m_current_version);
            InnerNode *new_node = version_split_inner_node(inner_node);

            int slot_of_sibling_in_parent_node = find_sibling_slot_to_merge(parent_node, slot_in_parent_node);
            InnerNode *sibling_inner = static_cast<InnerNode *>(parent_node->entries[slot_of_sibling_in_parent_node].ptr_child);
            bool sibling_key_is_smaller = key_less(sibling_inner->keyrange.min_key, new_node->keyrange.min_key) ? true : false;

            std::vector<InnerEntry> v_extra_entries;

            if (!new_node->is_strong_version_overflow(sibling_inner->alive_slot_size))
            {
                merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                Node *ptr_child = static_cast<Node *>(new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child);
                insert_one_delete_two(new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
            else
            {
                InnerNode *second_new_node = key_split_after_merge_inner(new_node, sibling_inner, sibling_key_is_smaller, v_extra_entries);
                Node *ptr_child_first = static_cast<Node *>(new_node);
                Node *ptr_child_second = static_cast<Node *>(second_new_node);
                InnerEntry new_parent_entry(new_node->keyrange, new_node->lifespan.start_version, ptr_child_first);
                InnerEntry second_new_parent_entry(second_new_node->keyrange, second_new_node->lifespan.start_version, ptr_child_second);
                insert_two_delete_two(new_parent_entry, second_new_parent_entry, slot_in_parent_node, slot_of_sibling_in_parent_node, path);
            }
        }

        void treat_root_inner_block_underflow(InnerNode *inner_node, const int slot_in_node)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_weak_version_underflow(-1), "Deleting 1 more entry would cause the node to weak version underflow.");

            inner_node->end_entry(slot_in_node, m_current_version);
        }

        void treat_root_inner_block_underflow_two(InnerNode *inner_node, const int slot_in_node, const int slot_of_sibling_in_node)
        {
            MVBTREE_ASSERT(!inner_node->is_leaf(), "The node should be an inner node.");
            MVBTREE_ASSERT(inner_node->is_weak_version_underflow(-2), "Deleting 2 more entry would cause the node to weak version underflow.");

            inner_node->end_entry(slot_in_node, m_current_version);
            inner_node->end_entry(slot_of_sibling_in_node, m_current_version);
        }

        void insert_one_delete_one(const InnerEntry &inner_entry, const int slot, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(!node->is_leaf(), "The expected deleted entry should be an inner entry.");
            InnerNode *inner_node = static_cast<InnerNode *>(node);

            
            inner_node->end_entry(slot, m_current_version);

            if (inner_node->is_block_overflow(1) && !is_root)
            {
                Node *parent = (path.empty()) ? m_root : path.top().second;
                InnerNode *parent_node = static_cast<InnerNode *>(parent);
                treat_nonroot_inner_block_overflow(inner_node, inner_entry, parent_node, slot_in_parent_node, path);
            }
            else if (inner_node->is_block_overflow(1) && is_root)
            {
                treat_root_inner_block_overflow(inner_node, inner_entry);
            }
            else
            {
                inner_node->insert_entry(inner_entry);
            }
        }

        void insert_one_delete_two(const InnerEntry &inner_entry, const int slot, const int slot_of_sibling, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(!node->is_leaf(), "The expected deleted entry should be an inner entry.");
            InnerNode *inner_node = static_cast<InnerNode *>(node);

            if (inner_node->is_block_overflow(1))
            {
                inner_node->end_entry(slot, m_current_version);
                inner_node->end_entry(slot_of_sibling, m_current_version);

                if (!is_root)
                {
                    Node *parent = (path.empty()) ? m_root : path.top().second;
                    InnerNode *parent_node = static_cast<InnerNode *>(parent);
                    treat_nonroot_inner_block_overflow(inner_node, inner_entry, parent_node, slot_in_parent_node, path);
                }
                else
                {
                    treat_root_inner_block_overflow(inner_node, inner_entry);
                }
            }
            else
            {
                inner_node->insert_entry(inner_entry);

                if (inner_node->is_weak_version_underflow(-2) && !is_root)
                {
                    Node *parent = (path.empty()) ? m_root : path.top().second;
                    InnerNode *parent_node = static_cast<InnerNode *>(parent);
                    treat_nonroot_inner_block_underflow_two(inner_node, slot, slot_of_sibling, parent_node, slot_in_parent_node, path);
                }
                else if (inner_node->is_weak_version_underflow(-2) && is_root)
                {
                    treat_root_inner_block_underflow_two(inner_node, slot, slot_of_sibling);
                }
                else
                {
                    inner_node->end_entry(slot, m_current_version);
                    inner_node->end_entry(slot_of_sibling, m_current_version);
                }
            }
        }

        void insert_two_delete_one(const InnerEntry &inner_entry, const InnerEntry &second_entry, const int slot, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(!node->is_leaf(), "The expected deleted entry should be an inner entry.");
            InnerNode *inner_node = static_cast<InnerNode *>(node);

            inner_node->end_entry(slot, m_current_version);

            if (inner_node->is_block_overflow(2) && !is_root)
            {
                Node *parent = (path.empty()) ? m_root : path.top().second;
                InnerNode *parent_node = static_cast<InnerNode *>(parent);
                treat_nonroot_inner_block_overflow_two(inner_node, inner_entry, second_entry, parent_node, slot_in_parent_node, path);
            }
            else if (inner_node->is_block_overflow(2) && is_root)
            {
                treat_root_inner_block_overflow_two(inner_node, inner_entry, second_entry);
            }
            else
            {
                inner_node->insert_entry(inner_entry);
                inner_node->insert_entry(second_entry);
            }
        }

        void insert_two_delete_two(const InnerEntry &inner_entry, const InnerEntry &second_entry, const int slot, const int slot_of_sibling, std::stack<std::pair<int, Node *>> &path)
        {
            bool is_root;
            int slot_in_parent_node = 0;
            Node *node = get_node_from_path(path, is_root, slot_in_parent_node);

            MVBTREE_ASSERT(!node->is_leaf(), "The expected deleted entry should be an inner entry.");
            InnerNode *inner_node = static_cast<InnerNode *>(node);

            if (inner_node->is_block_overflow(2))
            {
                inner_node->end_entry(slot, m_current_version);
                inner_node->end_entry(slot_of_sibling, m_current_version);

                if (!is_root)
                {
                    Node *parent = (path.empty()) ? m_root : path.top().second;
                    InnerNode *parent_node = static_cast<InnerNode *>(parent);
                    treat_nonroot_inner_block_overflow_two(inner_node, inner_entry, second_entry, parent_node, slot_in_parent_node, path);
                }
                else
                {
                    treat_root_inner_block_overflow_two(inner_node, inner_entry, second_entry);
                }
            }
            else
            {
                inner_node->insert_entry(inner_entry);
                inner_node->insert_entry(second_entry);

                if (inner_node->is_weak_version_underflow(-2) && !is_root)
                {
                    Node *parent = (path.empty()) ? m_root : path.top().second;
                    InnerNode *parent_node = static_cast<InnerNode *>(parent);
                    treat_nonroot_inner_block_underflow_two(inner_node, slot, slot_of_sibling, parent_node, slot_in_parent_node, path);
                }
                else if (inner_node->is_weak_version_underflow(-2) && is_root)
                {
                    treat_root_inner_block_underflow_two(inner_node, slot, slot_of_sibling);
                }
                else
                {
                    inner_node->end_entry(slot, m_current_version);
                    inner_node->end_entry(slot_of_sibling, m_current_version);
                }
            }
        }

#pragma endregion
    };

}

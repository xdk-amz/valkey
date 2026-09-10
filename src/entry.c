#include "server.h"
#include "serverassert.h"
#include "entry.h"
#include "intern.h"

/*-----------------------------------------------------------------------------
 * Entry Implementation
 *----------------------------------------------------------------------------*/

/* There are some different formats for the "entry".  In all cases, the "entry" pointer points into the
 * allocation and is identical to the "field" sds pointer.
 *
 * Type 1: Field sds type is an SDS_TYPE_5
 *     With this type, both the field and value are embedded in the entry.  Expiration is not allowed
 *     as the sdshdr5 (on field) doesn't contain any aux bits to encode the existence of an
 *     expiration.  Extra padding is included in the value to the size of the physical block.
 *
 *             entry
 *               |
 *     +---------V------------+----------------------------+
 *     |       Field          |      Value                 |
 *     | sdshdr5 | "foo" \0   | sdshdr8 "bar" \0 (padding) |
 *     +---------+------------+----------------------------+
 *
 *     Identified by: field sds type is SDS_TYPE_5
 *
 *
 * Type 2: Field sds type is an SDS_TYPE_8 type
 *     With this type, both the field and value are embedded.  Extra bits in the sdshdr8 (on field)
 *     are used to encode aux flags which may indicate the presence of an optional expiration.
 *     Extra padding is included in the value to the size of the physical block.
 *
 *                            entry
 *                              |
 *     +--------------+---------V------------+----------------------------+
 *     | Expire (opt) |       Field          |      Value                 |
 *     |  mstime_t    | sdshdr8 | "foo" \0   | sdshdr8 "bar" \0 (padding) |
 *     +--------------+---------+------------+----------------------------+
 *
 *     Identified by: field sds type is SDS_TYPE_8  AND  has embedded value
 *
 *
 * Type 3: Value is an sds, referenced by pointer
 *     With this type, the field is embedded, and the value is an sds, referenced by pointer.  Extra
 *     bits in the sdshdr8(+) are used to encode aux flags which indicate the presence of a value by
 *     pointer.  An aux bit may indicate the presence of an optional expiration.  Note that the
 *     "field" is not padded, so there's no direct way to identify the length of the allocation.
 *
 *                                             entry
 *                                               |
 *     +--------------+---------------+----------V----------+--------+
 *     | Expire (opt) |     Value     |        Field        | / / / /|
 *     |  mstime_t    | sds (pointer) | sdshdr8+ | "foo" \0 |/ / / / |
 *     +--------------+-------+-------+----------+----------+--------+
 *                            |
 *                            +-> sds value
 *
 *     Identified by: Does not have embedded value, value is an sds, referenced by pointer.
 *
 *
 * Type 4: Value is an stringRef, referenced by pointer
 *     With this type, the field is embedded, and the value is a stringRef, referenced by pointer.  Extra
 *     bits in the sdshdr8(+) are used to encode aux flags which indicate the presence of a value by
 *     pointer.  An aux bit may indicate the presence of an optional expiration.  Note that the
 *     "field" is not padded, so there's no direct way to identify the length of the allocation.
 *     This type is used when the entry's Value holds a buffer but doesn't own it, meaning
 *     freeing the entry doesn't involve freeing of the buffer.
 *
 *                                                   entry
 *                                                     |
 *     +--------------+---------------------+----------V----------+--------+
 *     | Expire (opt) |        Value        |        Field        | / / / /|
 *     |  mstime_t    | stringRef (pointer) | sdshdr8+ | "foo" \0 |/ / / / |
 *     +--------------+----------+----------+----------+----------+--------+
 *                               |
 *                               |
 *                               +-> stringRef value
 *                                              |
 *                             +----------------V---------------+
 *                             | buffer pointer | buffer length |
 *                             +----------------+---------------+
 *
 *     Identified by: Does not have embedded value, value is a stringRef, referenced by pointer.
 *
 *
 * Type 5: Interned field name (spike S2)
 *     The field name is NOT embedded; it is a shared, refcounted intern slot laid
 *     out as a genuine sdshdr8, referenced by pointer. The value is ALWAYS by
 *     pointer (stringRef, possibly also an intern handle) so the layout is finite.
 *     The entry* points just past a 1-byte entry FLAGS byte (so entry[-1] is the
 *     flags byte and entry[0] is a NUL). The flags byte's low 3 bits are
 *     SDS_TYPE_INTERN_FIELD (5); its high 5 bits carry the entry's own aux bits
 *     (expiry, value-ptr, string-ref, intern-handle) at the same shift the sds
 *     header uses, so entryGetAuxBit/entrySetAuxBit read the flags byte directly.
 *
 *                          entry
 *                            |
 *     +------+-------+--------+-------+----V--+
 *     | Exp? | value | field  | flags | \0    |
 *     |      | ptr   | slot   | byte  |       |
 *     +------+-------+--------+-------+-------+
 *                       |
 *                       +-> internSlot (sdshdr8 body is a genuine sds field name)
 *
 *     Identified by: (entry[-1] & SDS_TYPE_MASK) == SDS_TYPE_INTERN_FIELD.
 */

/* SDS types 0-4 are used by sds.h; 5-7 are free. We reuse type value 5 as the
 * discriminator in the Type-5 entry flags byte. */
#define SDS_TYPE_INTERN_FIELD 5

enum {
    /* SDS aux flag. If set, it indicates that the entry has TTL metadata set. */
    FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY = 0,
    /* SDS aux flag. If set, it indicates that the entry has an embedded value
     * pointer (sds or stringRef) located in memory before the embedded field. If unset, the entry
     * instead has an embedded value located after the embedded field. */
    FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR = 1,
    /* SDS aux flag. If set, it indicates that the entry has an embedded value
     * pointer (stringRef) located in memory before the embedded field.
     * If set, it indicates that the hash entry has a reference to the value.
     * The hash entry does not own the string reference and will not free it upon
     * entry destruction. The primary usecase is to avoid memory duplication
     * between the core and a module. */
    FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF = 2,
    /* SDS aux flag. If set (alongside HAS_VALUE_PTR and HAS_STRING_REF), the
     * value pointer is an internSlot* owned by the intern subsystem. The entry
     * releases the slot (refcount--) instead of freeing the pointer on
     * destruction or overwrite. All read paths treat it as a Type-4 stringRef. */
    FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE = 3,
    FIELD_SDS_AUX_BIT_MAX
};
static_assert(FIELD_SDS_AUX_BIT_MAX < sizeof(char) - SDS_TYPE_BITS, "too many sds bits are used for entry metadata");

/* Type-5 discriminator: the entry's flags byte lives at entry[-1]; its low 3
 * bits equal SDS_TYPE_INTERN_FIELD for an interned-field entry. For every other
 * entry type, entry[-1] is a real sds flags byte whose type is 0-4. */
bool entryFieldIsInterned(const entry *entry) {
    return (((const unsigned char *)entry)[-1] & SDS_TYPE_MASK) == SDS_TYPE_INTERN_FIELD;
}

/* Reads one of the entry's aux bits. For a Type-5 entry the bits live in the
 * entry flags byte (entry[-1]) at the same shift the sds header uses; otherwise
 * they live in the embedded field sds header. */
static int entryGetAuxBit(const entry *entry, int bit) {
    if (entryFieldIsInterned(entry)) {
        unsigned char flags = ((const unsigned char *)entry)[-1];
        return (flags >> (SDS_TYPE_BITS + bit)) & 1U;
    }
    return sdsGetAuxBit((sds)entry, bit);
}

/* Sets one of the entry's aux bits (see entryGetAuxBit). */
static void entrySetAuxBit(entry *entry, int bit, int value) {
    if (entryFieldIsInterned(entry)) {
        unsigned char *fb = &((unsigned char *)entry)[-1];
        if (value) {
            *fb |= (unsigned char)(1 << (SDS_TYPE_BITS + bit));
        } else {
            *fb &= (unsigned char)~(1 << (SDS_TYPE_BITS + bit));
        }
        return;
    }
    sdsSetAuxBit((sds)entry, bit, value);
}

/* Returns the field slot pointer of a Type-5 entry. Located just before the
 * flags byte: entry[-1] is flags, then a field-slot pointer, then a value ptr. */
/* The word before the flags byte holds the shared field SDS pointer (the sds
 * body inside the field slot), not the slot pointer: a hashtable probe then costs
 * one dependent load (entry -> field bytes) instead of two (entry -> slot -> bytes). */
static sds entryGetFieldSdsPtr(const entry *entry) {
    serverAssert(entryFieldIsInterned(entry));
    char *p = (char *)entry - 1 - sizeof(void *);
    return *(sds *)p;
}

static struct internSlot *entryGetFieldSlot(const entry *entry) {
    return internFieldSlotFromSds(entryGetFieldSdsPtr(entry));
}

/* The entry pointer is the field sds, except for a Type-5 entry, whose field is
 * a shared intern slot laid out as a genuine sdshdr8. */
sds entryGetField(const entry *entry) {
    if (entryFieldIsInterned(entry)) return entryGetFieldSdsPtr(entry);
    return (sds)entry;
}

/* Returns true in case the entry's value is not embedded in the entry.
 * Returns false otherwise. */
static bool entryHasValuePtr(const entry *entry) {
    return entryGetAuxBit(entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR);
}

static void **entryGetValueRef(const entry *entry);

/* Returns true in case the entry's value is embedded in the entry.
 * Returns false otherwise. */
bool entryHasEmbeddedValue(const entry *entry) {
    return (!entryHasValuePtr(entry));
}

/* Returns true in case the entry holds a stringRef reference of the value.
 * Returns false otherwise. */
bool entryHasStringRef(const entry *entry) {
    return entryHasValuePtr(entry) && entryGetAuxBit(entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF);
}

/* Returns true in case the entry's value pointer is an intern slot.
 * Returns false otherwise. An interned value is always also a stringRef. */
bool entryHasInternHandle(const entry *entry) {
    return entryHasValuePtr(entry) && entryGetAuxBit(entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE);
}

/* Returns the intern slot pointer of an interned value. */
/* An interned value is stored as the slot's inline sds pointer, so the read path
 * is the plain sds-by-pointer path (HAS_STRING_REF is NOT set); the slot header
 * is recovered from the sds for owner logic (release, mem-usage). */
static struct internSlot *entryGetInternSlot(const entry *entry) {
    serverAssert(entryHasInternHandle(entry));
    return internSlotFromSds((sds)*entryGetValueRef(entry));
}
/* Returns true in case the entry has expiration timestamp.
 * Returns false otherwise. */
bool entryHasExpiry(const entry *entry) {
    return entryGetAuxBit(entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY);
}

/* Returns the location of a pointer to a separately allocated value. Only for
 * an entry without an embedded value. */
static void **entryGetValueRef(const entry *entry) {
    serverAssert(entryHasValuePtr(entry));
    if (entryFieldIsInterned(entry)) {
        /* Type-5: [expiry?][value ptr][field slot ptr][flags]<-entry. The value
         * pointer sits two words before the flags byte. */
        char *p = (char *)entry - 1 - 2 * sizeof(void *);
        return (void **)p;
    }
    char *field_data = sdsAllocPtr(entryGetField(entry));
    field_data -= sizeof(void *);
    return (void **)field_data;
}

static sds *entryGetSdsValueRef(const entry *entry) {
    return (sds *)entryGetValueRef(entry);
}

static stringRef *entryGetStringRefRef(const entry *entry) {
    serverAssert(entryHasStringRef(entry));
    return (stringRef *)*entryGetValueRef(entry);
}

/* Returns the entry's value. */
char *entryGetValue(const entry *entry, size_t *len) {
    if (entryHasEmbeddedValue(entry)) {
        sds value;
        if (entryFieldIsInterned(entry)) {
            /* Type-5 embedded: the value sdshdr8 starts at entry[0]. */
            value = (char *)entry + sdsHdrSize(SDS_TYPE_8);
        } else {
            /* Skip field content, field null terminator and value sds8 hdr. */
            size_t offset = sdslen(entryGetField(entry)) + 1 + sdsHdrSize(SDS_TYPE_8);
            value = (char *)entry + offset;
        }
        if (len) *len = sdslen(value);
        return value;
    }
    if (entryHasStringRef(entry)) {
        stringRef *string_ref = entryGetStringRefRef(entry);
        if (!string_ref) return NULL;
        if (len) *len = string_ref->len;
        return (char *)string_ref->buf;
    }
    sds *value_ref = entryGetSdsValueRef(entry);
    if (len) *len = sdslen(*value_ref);
    return *value_ref;
}

/* Frees the entry's non-embedded value.
 * If the value is a string reference (stringRef), only the entry's pointer
 * is freed, as the underlying string is not owned by this entry.
 * Otherwise, the value is a standard SDS and is fully freed. */
static void entryFreeValuePtr(entry *entry) {
    serverAssert(entryHasValuePtr(entry));
    void **value_ref = entryGetValueRef(entry);
    if (entryHasInternHandle(entry)) {
        internRelease(internSlotFromSds((sds)*value_ref));
    } else if (entryHasStringRef(entry)) {
        zfree(*value_ref);
    } else {
        sdsfree(*value_ref);
    }
    *value_ref = NULL;
}

static void entrySetValueSds(entry *e, sds value) {
    serverAssert(entryHasValuePtr(e));
    entryFreeValuePtr(e);
    if (entryHasInternHandle(e)) entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE, 0);
    if (entryHasStringRef(e)) entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF, 0);
    sds *value_ref = entryGetSdsValueRef(e);
    *value_ref = value;
}

/* Returns the address of the entry allocation. */
static void *entryGetAllocPtr(const entry *entry) {
    if (entryFieldIsInterned(entry)) {
        /* Type-5: [expiry?][value ptr][field slot ptr][flags]<-entry. Back up the
         * flags byte, both pointers, and the optional expiry. */
        char *buf = (char *)entry - 1 - sizeof(void *); /* field slot ptr */
        if (entryHasValuePtr(entry)) buf -= sizeof(void *);   /* value ptr */
        if (entryHasExpiry(entry)) buf -= sizeof(mstime_t);
        return buf;
    }
    char *buf = sdsAllocPtr(entryGetField(entry));
    if (entryHasValuePtr(entry)) buf -= sizeof(void *);
    if (entryHasExpiry(entry)) buf -= sizeof(mstime_t);
    return buf;
}

/**************************************** Entry Expiry API *****************************************/
/* Returns the location of a pointer to the expiry */
static mstime_t *entryGetExpiryRef(const entry *entry) {
    serverAssert(entryHasExpiry(entry));
    char *buf = entryGetAllocPtr(entry);
    return (mstime_t *)buf;
}

/* Returns the entry expiration timestamp.
 * In case this entry has no expiration time, will return EXPIRE_NONE. */
mstime_t entryGetExpiry(const entry *entry) {
    if (entryHasExpiry(entry)) return *entryGetExpiryRef(entry);
    return EXPIRY_NONE;
}

/* Modify the expiration time of this entry and return a pointer to the (potentially new) entry. */
entry *entrySetExpiry(entry *e, mstime_t expiry) {
    if (expiry != EXPIRY_NONE && entryHasExpiry(e)) {
        *entryGetExpiryRef(e) = expiry;
        return e;
    }
    if (entryHasInternHandle(e)) {
        struct internSlot *own = entryGetInternSlot(e);
        internRetain(own); /* entryUpdateAsInternHandle consumes one reference */
        return entryUpdateAsInternHandle(e, own, expiry);
    }
    if (entryHasStringRef(e)) {
        stringRef *value = entryGetStringRefRef(e);
        return entryUpdateAsStringRef(e, value->buf, value->len, expiry);
    }
    return entryUpdate(e, NULL, expiry);
}

/* Return true in case the entry has assigned expiration and has already expired,
 * or false otherwise. */
bool entryIsExpired(entry *entry) {
    mstime_t entry_expiry = entryGetExpiry(entry);
    if (entry_expiry == EXPIRY_NONE) return false;
    return timestampIsExpired(entry_expiry);
}
/**************************************** Entry Expiry API - End *****************************************/

void entryFree(entry *entry) {
    if (entryHasValuePtr(entry)) entryFreeValuePtr(entry);
    if (entryFieldIsInterned(entry)) internReleaseField(entryGetFieldSlot(entry));
    zfree(entryGetAllocPtr(entry));
}

static inline size_t entryReqSize(size_t field_len,
                                  size_t value_len,
                                  mstime_t expiry,
                                  bool *is_value_embedded,
                                  int *field_sds_type,
                                  size_t *field_size,
                                  size_t *expiry_size,
                                  size_t *embedded_value_size) {
    size_t expiry_alloc_size = (expiry == EXPIRY_NONE) ? 0 : sizeof(mstime_t);
    int embedded_field_sds_type = sdsReqType(field_len);
    if (embedded_field_sds_type == SDS_TYPE_5 && (expiry_alloc_size > 0)) {
        embedded_field_sds_type = SDS_TYPE_8;
    }
    size_t field_alloc_size = sdsReqSize(field_len, embedded_field_sds_type);
    size_t embedded_value_alloc_size = value_len != SIZE_MAX ? sdsReqSize(value_len, SDS_TYPE_8) : 0;
    size_t alloc_size = field_alloc_size + expiry_alloc_size;
    bool embed_value = false;
    if (value_len != SIZE_MAX) {
        if (alloc_size + embedded_value_alloc_size <= EMBED_VALUE_MAX_ALLOC_SIZE) {
            /* Embed field and value. Value is fixed to SDS_TYPE_8. Unused
             * allocation space is recorded in the embedded value's SDS header.
             *
             *     +------+--------------+---------------+
             *     | TTL  | field        | value         |
             *     |      | hdr "foo" \0 | hdr8 "bar" \0 |
             *     +------+--------------+---------------+
             */
            embed_value = true;
            alloc_size += embedded_value_alloc_size;
        } else {
            /* Embed field, but not value. Field must be >= SDS_TYPE_8 to encode to
             * indicate this type of entry.
             *
             *     +------+-------+---------------+
             *     | TTL  | value | field         |
             *     |      | ptr   | hdr8 "foo" \0 |
             *     +------+-------+---------------+
             */
            embed_value = false;
            alloc_size += sizeof(sds);
            if (embedded_field_sds_type == SDS_TYPE_5) {
                embedded_field_sds_type = SDS_TYPE_8;
                alloc_size -= field_alloc_size;
                field_alloc_size = sdsReqSize(field_len, embedded_field_sds_type);
                alloc_size += field_alloc_size;
            }
        }
    }
    if (expiry_size) *expiry_size = expiry_alloc_size;
    if (field_sds_type) *field_sds_type = embedded_field_sds_type;
    if (field_size) *field_size = field_alloc_size;
    if (embedded_value_size) *embedded_value_size = embedded_value_alloc_size;
    if (is_value_embedded) *is_value_embedded = embed_value;

    return alloc_size;
}

/* Serialize the content of the entry into an allocated buffer buf.
 * Note that this function will take ownership of the value so user should not assume it is valid after this call. */
static entry *entryConstruct(size_t alloc_size,
                             const_sds field,
                             sds sds_value,
                             stringRef *stringref_value,
                             mstime_t expiry,
                             bool embed_value,
                             int embedded_field_sds_type,
                             size_t expiry_size,
                             size_t embedded_value_sds_size,
                             size_t embedded_field_sds_size) {
    serverAssert((sds_value == NULL && stringref_value == NULL && embed_value) || (sds_value != NULL && stringref_value == NULL) || (sds_value == NULL && stringref_value != NULL && !embed_value));
    size_t buf_size;
    /* allocate the buffer */
    char *buf = zmalloc_usable(alloc_size, &buf_size);

    /* Set the expiry if exists */
    if (expiry_size) {
        *(mstime_t *)buf = expiry;
        buf += expiry_size;
        buf_size -= expiry_size;
    }
    /* Set the value if exists */
    if (!embed_value) {
        /* The value is not embedded, the value pointer is written before the field data. */
        *(void **)buf = sds_value ? (void *)sds_value : (void *)stringref_value;
        buf += sizeof(void *);
        buf_size -= sizeof(void *);
    } else if (sds_value) {
        /* The value is embedded, the value data is written after the field data. */
        sdswrite(buf + embedded_field_sds_size, buf_size - embedded_field_sds_size, SDS_TYPE_8, sds_value, sdslen(sds_value));
        sdsfree(sds_value);
        buf_size -= embedded_value_sds_size;
    }
    /* Set the field data.  When we write the field into the buffer, the entry pointer is the returned
     * sds (after the sds header). */
    entry *new_entry = (entry *)sdswrite(buf, embedded_field_sds_size, embedded_field_sds_type, field, sdslen(field));

    /* Field sds aux bits are zero, which we use for this entry encoding. */
    sdsSetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR, embed_value ? 0 : 1);
    sdsSetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY, expiry_size > 0 ? 1 : 0);

    /* Check that the new entry was built correctly */
    debugServerAssert(sdsGetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR) == (embed_value ? 0 : 1));
    debugServerAssert(sdsGetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY) == (expiry_size > 0 ? 1 : 0));
    return new_entry;
}

/* Probe path for entryCreate (P2 S4a). Returns a probe-interned entry when
 * string-interning is in probe mode and the field and/or value hit the dedup
 * index; otherwise returns NULL and entryCreate builds the stock entry. Takes
 * ownership of `value` exactly as entryCreate does (freed or embedded here). */
static entry *entryCreateProbe(const_sds field, sds value, mstime_t expiry);

/* Takes ownership of value. does not take ownership of field */
entry *entryCreate(const_sds field, sds value, mstime_t expiry) {
    /* Write-time probe admission (P2 S4a): in probe mode, look the field and
     * value up in the dedup index (lookup-only) and, on a hit, build an entry
     * that shares the interned slot(s) instead of a private copy. A probe hit
     * hands us ONE reference that the constructed entry owns. */
    entry *pe = entryCreateProbe(field, value, expiry);
    if (pe != NULL) return pe;

    bool embed_value = false;
    int embedded_field_sds_type;
    size_t expiry_size, embedded_value_sds_size, embedded_field_sds_size;
    size_t value_len = value ? sdslen(value) : SIZE_MAX;
    size_t alloc_size = entryReqSize(sdslen(field), value_len, expiry, &embed_value, &embedded_field_sds_type, &embedded_field_sds_size, &expiry_size, &embedded_value_sds_size);
    return entryConstruct(alloc_size, field, value, NULL, expiry, embed_value, embedded_field_sds_type, expiry_size, embedded_value_sds_size, embedded_field_sds_size);
}

/* Value kinds a Type-5 entry can carry. ENTRY_VAL_SDS is embedded after the
 * flags byte when the whole entry fits EMBED_VALUE_MAX_ALLOC_SIZE, else by pointer. */
typedef enum { ENTRY_VAL_SDS, ENTRY_VAL_STRINGREF, ENTRY_VAL_INTERN } entryValueKind;

/* True if a Type-5 entry with an owned sds value of this length embeds it. */
static bool entryInternFieldEmbeds(size_t value_len, mstime_t expiry) {
    size_t expiry_size = (expiry == EXPIRY_NONE) ? 0 : sizeof(mstime_t);
    return expiry_size + sizeof(void *) + 1 + sdsReqSize(value_len, SDS_TYPE_8) <= EMBED_VALUE_MAX_ALLOC_SIZE;
}

/* Builds a Type-5 interned-field entry. Layout, low address to high:
 *   [expiry (opt, 8)] [value ptr (8)] [field slot ptr (8)] [flags byte (1)] [NUL (1)]
 * The returned entry* points at the NUL byte; entry[-1] is the flags byte whose
 * low 3 bits are SDS_TYPE_INTERN_FIELD and whose high bits carry the aux bits.
 * `value` is stored by pointer as-is: an owned sds, a stringRef, or an internSlot
 * per `kind`. The entry takes ownership of the field slot reference the caller
 * passes (refcount is not bumped here). */
static entry *entryConstructInternField(struct internSlot *fieldSlot, void *value, entryValueKind kind, mstime_t expiry) {
    size_t expiry_size = (expiry == EXPIRY_NONE) ? 0 : sizeof(mstime_t);
    bool embed = (kind == ENTRY_VAL_SDS) && entryInternFieldEmbeds(sdslen((sds)value), expiry);
    size_t head_size = expiry_size + (embed ? 1 : 2) * sizeof(void *) + 1 /* flags */;
    size_t alloc_size = head_size + (embed ? sdsReqSize(sdslen((sds)value), SDS_TYPE_8) : 1 /* NUL */);
    size_t buf_size;
    char *buf = zmalloc_usable(alloc_size, &buf_size);
    char *p = buf;
    if (expiry_size) {
        *(mstime_t *)p = expiry;
        p += expiry_size;
    }
    if (!embed) {
        *(void **)p = value; /* value pointer (owned sds, stringRef, or internSlot) */
        p += sizeof(void *);
    }
    *(sds *)p = internSlotFieldSds(fieldSlot); /* shared field sds (slot recoverable via internFieldSlotFromSds) */
    p += sizeof(void *);
    /* Flags byte: type = SDS_TYPE_INTERN_FIELD, aux bits set below. */
    *(unsigned char *)p = SDS_TYPE_INTERN_FIELD;
    p += 1;
    entry *new_entry = (entry *)p;
    if (embed) {
        /* Embedded value: an sdshdr8 starting at entry[0], padded to the block. */
        sds v = (sds)value;
        sdswrite(p, buf_size - head_size, SDS_TYPE_8, v, sdslen(v));
        sdsfree(v);
    } else {
        *p = '\0'; /* NUL at entry[0] so an accidental %s prints empty */
    }

    entrySetAuxBit(new_entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR, embed ? 0 : 1);
    entrySetAuxBit(new_entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF, kind == ENTRY_VAL_STRINGREF ? 1 : 0);
    entrySetAuxBit(new_entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE, kind == ENTRY_VAL_INTERN ? 1 : 0);
    entrySetAuxBit(new_entry, FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY, expiry_size ? 1 : 0);
    return new_entry;
}

/* See the forward declaration near entryCreate. */
static entry *entryCreateProbe(const_sds field, sds value, mstime_t expiry) {
    if (server.string_interning != INTERN_MODE_PROBE) return NULL;

    struct internSlot *fslot = internProbeField((const char *)field, sdslen(field));
    struct internSlot *vslot = value ? internProbeValue(value, sdslen(value)) : NULL;

    if (fslot == NULL && vslot == NULL) return NULL; /* miss/miss: stock path */

    if (fslot != NULL && vslot != NULL) {
        /* hit/hit: Type-5 entry whose value shares the interned value slot. Both
         * probes handed us one reference; entryConstructInternField takes the
         * field slot reference, and ENTRY_VAL_INTERN carries the value slot's
         * sds (its reference owned by the entry). The caller's `value` copy is
         * now redundant -- entryCreate owns it, so free it. */
        entry *e = entryConstructInternField(fslot, internSlotSds(vslot), ENTRY_VAL_INTERN, expiry);
        sdsfree(value);
        return e;
    }

    if (fslot != NULL) {
        /* field-hit only: Type-5 entry with an owned sds value (embedded per
         * S2e when it fits). entryConstructInternField takes ownership of both
         * the field slot reference and the owned `value` sds. */
        return entryConstructInternField(fslot, value, ENTRY_VAL_SDS, expiry);
    }

    /* value-hit only: a stock (non-Type-5) entry whose value references the
     * interned value slot. Build the stock entry with the owned value, then let
     * entryUpdateAsInternHandle swap in the slot (it consumes the probe's one
     * reference and frees the owned value). */
    bool embed_value = false;
    int embedded_field_sds_type;
    size_t expiry_size, embedded_value_sds_size, embedded_field_sds_size;
    size_t alloc_size = entryReqSize(sdslen(field), sdslen(value), expiry, &embed_value, &embedded_field_sds_type,
                                     &embedded_field_sds_size, &expiry_size, &embedded_value_sds_size);
    entry *e = entryConstruct(alloc_size, field, value, NULL, expiry, embed_value, embedded_field_sds_type,
                              expiry_size, embedded_value_sds_size, embedded_field_sds_size);
    return entryUpdateAsInternHandle(e, vslot, expiry);
}

/* Converts an existing (non-Type-5) entry to a Type-5 interned-field entry. See
 * the header comment. The field slot's content must equal the entry's field. */
entry *entryConvertToInternField(entry *e, struct internSlot *fieldSlot) {
    serverAssert(!entryFieldIsInterned(e));
    mstime_t expiry = entryGetExpiry(e);

    /* Determine the value to carry. Type-5 is always value-by-pointer. */
    void *value_ptr;
    entryValueKind kind;
    if (entryHasInternHandle(e)) {
        /* Keep the same value slot: bump so entryFree(e) below does not free it. */
        struct internSlot *vslot = entryGetInternSlot(e);
        vslot->refcount++;
        value_ptr = internSlotSds(vslot);
        kind = ENTRY_VAL_INTERN;
    } else {
        /* Copy the value bytes into an owned sds so the new entry owns a
         * value-by-pointer independent of the old entry (freed via sdsfree). */
        size_t vlen;
        const char *vbuf = entryGetValue(e, &vlen);
        value_ptr = sdsnewlen(vbuf, vlen);
        kind = ENTRY_VAL_SDS;
    }

    entry *new_entry = entryConstructInternField(fieldSlot, value_ptr, kind, expiry);
    entryFree(e);
    return new_entry;
}

/* Rebuilds a Type-5 entry into a fresh Type-5 entry with the given value and
 * expiry (used when expiry toggles on/off, which changes the allocation size).
 * The field slot and its refcount are carried across unchanged; the value is
 * taken over (an owned sds `new_value`, or the entry's existing value when
 * new_value is NULL). Frees the old entry. */
static entry *entryRebuildInternField(entry *e, sds new_value, mstime_t expiry) {
    serverAssert(entryFieldIsInterned(e));
    struct internSlot *fieldSlot = entryGetFieldSlot(e);
    /* Guard the field slot across entryFree(e) so a transient refcount 0 cannot
     * free it. Net refcount unchanged. */
    fieldSlot->refcount++;

    void *value_ptr;
    entryValueKind kind;
    if (new_value != NULL) {
        value_ptr = new_value; /* owned sds, take ownership */
        kind = ENTRY_VAL_SDS;
    } else if (entryHasInternHandle(e)) {
        struct internSlot *vslot = entryGetInternSlot(e);
        vslot->refcount++; /* guard across entryFree(e) */
        value_ptr = internSlotSds(vslot);
        kind = ENTRY_VAL_INTERN;
    } else {
        /* Copy the current value into a fresh owned sds. */
        size_t vlen;
        const char *vbuf = entryGetValue(e, &vlen);
        value_ptr = sdsnewlen(vbuf, vlen);
        kind = ENTRY_VAL_SDS;
    }

    entry *new_entry = entryConstructInternField(fieldSlot, value_ptr, kind, expiry);
    entryFree(e);
    /* no refcount--: the guard reference is now owned by new_entry (entryFree released e's). */
    return new_entry;
}

/* Sets the entry's value to a string reference object.
 * The reference points to the provided `buf` but does not assume ownership.
 * It is assumed that an external mechanism will handle releasing any memory which
 * may have been associated with value->buf */
entry *entryUpdateAsStringRef(entry *e, const char *buf, size_t len, mstime_t expiry) {
    mstime_t entry_expiry = entryGetExpiry(e);
    // Check for toggling expiration
    bool expiry_add_remove = (expiry != entry_expiry) && (entry_expiry == EXPIRY_NONE || expiry == EXPIRY_NONE);
    if (entryHasValuePtr(e) && !expiry_add_remove) {
        if (entryHasStringRef(e)) {
            stringRef *value = entryGetStringRefRef(e);
            value->buf = buf;
            value->len = len;
        } else {
            stringRef *value = zmalloc(sizeof(stringRef));
            value->buf = buf;
            value->len = len;
            sds *value_ref = entryGetSdsValueRef(e);
            sdsfree(*value_ref);
            *value_ref = (sds)value;
            entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF, 1);
        }
        if (expiry != EXPIRY_NONE) *entryGetExpiryRef(e) = expiry;
        return e;
    }
    /* Rebuild needed (expiry toggles). A Type-5 entry stays Type-5. */
    if (entryFieldIsInterned(e)) {
        stringRef *value = zmalloc(sizeof(stringRef));
        value->buf = buf;
        value->len = len;
        struct internSlot *fieldSlot = entryGetFieldSlot(e);
        fieldSlot->refcount++;
        entry *new_entry = entryConstructInternField(fieldSlot, value, ENTRY_VAL_STRINGREF, expiry);
        entryFree(e);
        /* no refcount--: the guard reference is now owned by new_entry (entryFree released e's). */
        return new_entry;
    }
    stringRef *value = zmalloc(sizeof(stringRef));
    value->buf = buf;
    value->len = len;
    sds field = entryGetField(e);
    size_t field_size = sdsReqSize(sdslen(field), SDS_TYPE_8);
    size_t alloc_size = field_size + sizeof(void *);
    alloc_size += (expiry == EXPIRY_NONE) ? 0 : sizeof(expiry);

    size_t expiry_size = 0;
    if (expiry != EXPIRY_NONE) expiry_size = sizeof(expiry);
    entry *new_entry = entryConstruct(alloc_size, field, NULL, value, expiry, false, SDS_TYPE_8, expiry_size, sizeof(value), field_size);
    entryFree(e);

    sdsSetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF, 1);
    return new_entry;
}

/* Sets the entry's value to reference an intern slot. The slot pointer is stored
 * where the value pointer lives; the slot is prefix-compatible with stringRef so
 * all read paths see a Type-4 {buf,len} value. The entry does not own the slot's
 * bytes and releases it via internRelease on destruction/overwrite.
 *
 * CONTRACT: the caller transfers exactly ONE reference on `slot` (from
 * internAcquire or internRetain) to the entry. Whatever value reference the
 * entry held before is released -- including a reference on the same slot, in
 * which case the caller's reference is the surplus. Cloned from
 * entryUpdateAsStringRef. */
entry *entryUpdateAsInternHandle(entry *e, struct internSlot *slot, mstime_t expiry) {
    mstime_t entry_expiry = entryGetExpiry(e);
    bool expiry_add_remove = (expiry != entry_expiry) && (entry_expiry == EXPIRY_NONE || expiry == EXPIRY_NONE);
    if (entryHasValuePtr(e) && !expiry_add_remove) {
        /* In-place: overwrite the value pointer with the slot. */
        void **value_ref = entryGetValueRef(e);
        if (entryHasInternHandle(e)) {
            /* Release the reference the entry held: either a different slot,
             * or the same slot (then the caller's reference is the surplus). */
            internRelease(internSlotFromSds((sds)*value_ref));
        } else if (entryHasStringRef(e)) {
            zfree(*value_ref);
            entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_STRING_REF, 0);
            entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE, 1);
        } else {
            sdsfree(*value_ref);
            entrySetAuxBit(e, FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE, 1);
        }
        *value_ref = internSlotSds(slot);
        if (expiry != EXPIRY_NONE) *entryGetExpiryRef(e) = expiry;
        return e;
    }

    /* Rebuild: expiry is being toggled on/off, so the entry allocation changes.
     * A Type-5 entry stays Type-5. Guard the value slot across entryFree (which
     * releases the old value: the same slot when the entry was already interned)
     * so a transient refcount 0 never frees the slot we are about to store. */
    if (entryFieldIsInterned(e)) {
        struct internSlot *fieldSlot = entryGetFieldSlot(e);
        fieldSlot->refcount++; /* new_entry's reference; entryFree(e) below releases e's */
        entry *new_entry = entryConstructInternField(fieldSlot, internSlotSds(slot), ENTRY_VAL_INTERN, expiry);
        entryFree(e); /* releases e's value reference; the caller's keeps slot alive */
        /* no refcount--: the guard reference is now owned by new_entry (entryFree released e's). */
        return new_entry;
    }
    sds field = entryGetField(e);
    size_t field_size = sdsReqSize(sdslen(field), SDS_TYPE_8);
    size_t alloc_size = field_size + sizeof(void *);
    size_t expiry_size = 0;
    if (expiry != EXPIRY_NONE) {
        alloc_size += sizeof(expiry);
        expiry_size = sizeof(expiry);
    }
    entry *new_entry = entryConstruct(alloc_size, field, NULL, (stringRef *)internSlotSds(slot), expiry, false, SDS_TYPE_8, expiry_size, sizeof(void *), field_size);
    entryFree(e); /* releases e's value reference; the caller's keeps slot alive */

    sdsSetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_INTERN_HANDLE, 1);
    return new_entry;
}

/* Modify the entry's value and/or expiration time.
 * In case the provided value is NULL, will use the existing value.
 * Note that the value ownership is moved to this function and the caller should assume the
 * value is no longer usable after calling this function. */
entry *entryUpdate(entry *e, sds value, mstime_t expiry) {
    sds field = entryGetField(e);
    entry *new_entry = NULL;

    /* Write-time probe admission (P2 S4a): a value overwrite in probe mode
     * probes the value dedup index. On a hit, share the interned slot (the probe
     * hands one reference; entryUpdateAsInternHandle consumes it and handles both
     * Type-5 and stock entries, in place or by rebuild) and free the caller's
     * owned value. On a miss, fall through to the stock update. */
    if (value != NULL && server.string_interning == INTERN_MODE_PROBE) {
        struct internSlot *vslot = internProbeValue(value, sdslen(value));
        if (vslot != NULL) {
            entry *ne = entryUpdateAsInternHandle(e, vslot, expiry);
            sdsfree(value);
            return ne;
        }
    }

    /* Type-5 interned-field entry: keep it Type-5 across value/expiry updates.
     * The value is always by pointer, so no embedding decision applies. */
    if (entryFieldIsInterned(e)) {
        mstime_t curr_expiry = entryGetExpiry(e);
        bool update_value = value != NULL;
        bool update_expiry = expiry != curr_expiry;
        bool expiry_add_remove = update_expiry && (curr_expiry == EXPIRY_NONE || expiry == EXPIRY_NONE);
        if (!update_value && !update_expiry) return e;
        if (expiry_add_remove) {
            /* Reallocate (expiry toggles). Carry the new value if any. */
            return entryRebuildInternField(e, update_value ? value : NULL, expiry);
        }
        if (update_value) {
            bool new_embeds = entryInternFieldEmbeds(sdslen(value), expiry);
            if (entryHasEmbeddedValue(e) && new_embeds) {
                /* Embedded -> embedded: reuse the allocation when the new value fits
                 * the space the old one occupied (same rule as entryUpdate below). */
                char *old_value = entryGetValue(e, NULL);
                size_t value_space = sdsHdrSize(SDS_TYPE_8) + sdsalloc(old_value) + 1;
                if (sdsReqSize(sdslen(value), SDS_TYPE_8) <= value_space &&
                    sdsReqSize(sdslen(value), SDS_TYPE_8) >= value_space * 3 / 4) {
                    if (update_expiry) *entryGetExpiryRef(e) = expiry;
                    sdswrite(sdsAllocPtr(old_value), value_space, SDS_TYPE_8, value, sdslen(value));
                    sdsfree(value);
                    return e;
                }
                return entryRebuildInternField(e, value, expiry);
            }
            if (!entryHasEmbeddedValue(e) && !new_embeds) {
                /* Pointer -> pointer: swap the value pointer in place. */
                if (update_expiry) *entryGetExpiryRef(e) = expiry;
                entrySetValueSds(e, value);
                return e;
            }
            return entryRebuildInternField(e, value, expiry); /* encoding changes */
        }
        /* Expiry-only update with expiry already present. */
        *entryGetExpiryRef(e) = expiry;
        return e;
    }

    /* Expiry-only change of an interned or string-ref entry (value unchanged). */
    if (entryHasInternHandle(e) && !value) {
        struct internSlot *own = entryGetInternSlot(e);
        internRetain(own); /* entryUpdateAsInternHandle consumes one reference */
        return entryUpdateAsInternHandle(e, own, expiry);
    }
    if (entryHasStringRef(e) && !value) {
        stringRef *value = entryGetStringRefRef(e);
        return entryUpdateAsStringRef(e, value->buf, value->len, expiry);
    }
    bool update_value = value ? true : false;
    mstime_t curr_expiration_time = entryGetExpiry(e);
    bool update_expiry = (expiry != curr_expiration_time) ? true : false;
    /* Just a sanity check. If nothing changes, lets just return */
    if (!update_value && !update_expiry) return e;
    size_t value_len = SIZE_MAX;
    if (value) {
        value_len = sdslen(value);
    } else {
        value = entryGetValue(e, &value_len);
    }
    bool embed_value = false;
    int embedded_field_sds_type;
    size_t expiry_size, embedded_value_size, embedded_field_size;
    size_t required_entry_size = entryReqSize(sdslen(field), value_len, expiry, &embed_value, &embedded_field_sds_type, &embedded_field_size, &expiry_size, &embedded_value_size);
    size_t current_embedded_allocation_size = entryHasEmbeddedValue(e) ? entryMemUsage(e) : 0;

    bool expiry_add_remove = update_expiry && (curr_expiration_time == EXPIRY_NONE || expiry == EXPIRY_NONE); // In case we are toggling expiration
    bool value_change_encoding = update_value && (embed_value != entryHasEmbeddedValue(e));                   // In case we change the way value is embedded or not


    /* We will create a new entry in the following cases:
     * 1. In the case were we add or remove expiration.
     * 2. We change the way value is encoded
     * 3. in the case were we are NOT migrating from an embedded entry to an embedded entry with ~the same size. */
    bool create_new_entry = (expiry_add_remove) || (value_change_encoding) ||
                            (update_value && entryHasEmbeddedValue(e) &&
                             !(required_entry_size <= EMBED_VALUE_MAX_ALLOC_SIZE &&
                               required_entry_size <= current_embedded_allocation_size &&
                               required_entry_size >= current_embedded_allocation_size * 3 / 4));

    if (!create_new_entry) {
        /* In this case we are sure we do not have to allocate new entry, so expiry must already be set. */
        if (update_expiry) {
            serverAssert(entryHasExpiry(e));
            char *buf = entryGetAllocPtr(e);
            *(mstime_t *)buf = expiry;
        }
        /* In this case we are sure we do not have to allocate new entry, so value must already be set or we have enough room to embed it. */
        if (update_value) {
            if (entryHasEmbeddedValue(e)) {
                /* Skip field content, field null terminator and value sds8 hdr. */
                char *old_value = entryGetValue(e, NULL);
                /* We are using the same entry memory in order to store a potentially new value.
                 * In such cases the old value alloc was adjusted to the real buffer size part it was embedded to.
                 * Since we can potentially write here a smaller value, which requires less allocation space, we would like to
                 * inherit the old value memory allocation size. */
                size_t value_size = sdsHdrSize(SDS_TYPE_8) + sdsalloc(old_value) + 1;
                sdswrite(sdsAllocPtr(old_value), value_size, SDS_TYPE_8, value, sdslen(value));
                sdsfree(value);
            } else {
                entrySetValueSds(e, value);
            }
        }
        new_entry = e;

    } else {
        if (!update_value) {
            /* Check if the value can be reused. In case the original entry value is
             * embedded WE WILL HAVE TO DUPLICATE IT if not we have to duplicate it,
             * remove it from the original entry since we are going to delete it. */
            if (entryHasEmbeddedValue(e)) {
                value = sdsdup(value);
            } else {
                void **value_ref = entryGetValueRef(e);
                *value_ref = NULL;
            }
        }
        /* allocate the buffer for a new entry */
        new_entry = entryConstruct(required_entry_size, field, value, NULL, expiry, embed_value, embedded_field_sds_type, expiry_size, embedded_value_size, embedded_field_size);
        entryFree(e);
    }
    /* Check that the new entry was built correctly */
    debugServerAssert(sdsGetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_VALUE_PTR) == (embed_value ? 0 : 1));
    debugServerAssert(sdsGetAuxBit(entryGetField(new_entry), FIELD_SDS_AUX_BIT_ENTRY_HAS_EXPIRY) == (expiry_size > 0 ? 1 : 0));
    serverAssert(new_entry);
    return new_entry;
}

/* Returns memory usage of an entry, including all allocations owned by
 * the entry. */
size_t entryMemUsage(entry *entry) {
    size_t mem = 0;

    if (entryHasEmbeddedValue(entry) && !entryFieldIsInterned(entry)) {
        mem += sdsReqSize(sdslen(entryGetField(entry)), sdsType(entryGetField(entry)));
        if (entryHasExpiry(entry)) mem += sizeof(mstime_t);
    } else {
        /* In case the value is not embedded we might not be able to sum all the allocation sizes since the field
         * header could be too small for holding the real allocation size. */
        mem += zmalloc_usable_size(entryGetAllocPtr(entry));
    }
    if (entryHasInternHandle(entry)) {
        struct internSlot *slot = entryGetInternSlot(entry);
        mem += sizeof(void *) + internSlotAllocSize(slot) / internSlotRefcount(slot);
    } else if (entryHasStringRef(entry)) {
        mem += zmalloc_usable_size(entryGetStringRefRef(entry));
    } else if (entryHasValuePtr(entry) || !entryFieldIsInterned(entry)) {
        mem += sdsAllocSize((sds)entryGetValue(entry, NULL));
    } /* else: Type-5 embedded value is inside the entry allocation, counted above */
    /* Type-5: add the amortized share of the shared field slot (the honest
     * per-key number: one pointer in the entry + slot bytes / references). */
    if (entryFieldIsInterned(entry)) {
        struct internSlot *fslot = entryGetFieldSlot(entry);
        mem += sizeof(void *) + internSlotFieldAllocSize(fslot) / internSlotRefcount(fslot);
    }
    return mem;
}

/* Defragments an entry (field-value pair) if needed, using the
 * provided defrag functions. The defrag functions return NULL if the allocation
 * was not moved, otherwise they return a pointer to the new memory location.
 * A separate sds defrag function is needed because of the unique memory layout
 * of sds strings.
 * If the location of the entry changed we return the new location,
 * otherwise we return NULL. */
entry *entryDefrag(entry *e, void *(*defragfn)(void *), sds (*sdsdefragfn)(sds)) {
    if (entryHasInternHandle(e)) {
        /* The intern slot never moves; leave the value pointer untouched. */
    } else if (entryHasStringRef(e)) {
        stringRef **value_ref = (stringRef **)entryGetValueRef(e);
        stringRef *new_value = defragfn(*value_ref);
        if (new_value) *value_ref = new_value;
    } else if (entryHasValuePtr(e)) {
        sds *value_ref = (sds *)entryGetValueRef(e);
        sds new_value = sdsdefragfn(*value_ref);
        if (new_value) *value_ref = new_value;
    }
    char *allocation = entryGetAllocPtr(e);
    char *new_allocation = defragfn(allocation);
    if (new_allocation != NULL) {
        /* Return the same offset into the new allocation as the entry's offset
         * in the old allocation. */
        int entry_pointer_offset = (char *)e - allocation;
        return (entry *)(new_allocation + entry_pointer_offset);
    }
    return NULL;
}

/* Used for releasing memory to OS to avoid unnecessary CoW. Called when we've
 * forked and memory won't be used again. See zmadvise_dontneed() */
void entryDismissMemory(entry *entry) {
    /* Only dismiss values memory since the field size usually is small. */
    if (entryHasValuePtr(entry) && !entryHasStringRef(entry) && !entryHasInternHandle(entry)) dismissSds(*entryGetValueRef(entry));
}

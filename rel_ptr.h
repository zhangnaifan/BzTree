#ifndef REL_PTR_H
#define REL_PTR_H

/* relative pointer */
template<typename T>
class rel_ptr
{
	/* base address initialized in the start */
	static UCHAR * base_address;
	static PMEMoid base_oid;
	/* offset from the base address */
	uint64_t off;
public:
	/* constructors */
	rel_ptr() : off(0) {}
	rel_ptr(const T * abs_ptr) : off((UCHAR*)abs_ptr - base_address) {}
	explicit rel_ptr(uint64_t rel_addr) : off(rel_addr) {}
	template<typename U>
	rel_ptr(rel_ptr<U> rptr) : off((UCHAR*)rptr.abs() - base_address) {}
	rel_ptr(PMEMoid oid) : off((UCHAR*)pmemobj_direct(oid) - base_address) {}

	/* return coresponding PMEMoid */
	PMEMoid oid() { return { base_oid.pool_uuid_lo, base_oid.off + off }; }

	/* basic pointer usage */
	T& operator *() { return *(T*)(base_address + off); }
	T* operator ->() { return (T*)(base_address + off); }

	/* return absolute or relative address */
	uint64_t * abs() { return (uint64_t*)(base_address + off); }
	uint64_t rel() { return off; }

	/* comparison */
	bool operator ==(const rel_ptr<T>& rptr) { return off == rptr.off; }
	bool operator <(const rel_ptr<T>& rptr) { return off < rptr.off; }
	bool operator >(const rel_ptr<T>& rptr) { return off > rptr.off; }
	bool operator !=(const rel_ptr<T> & rptr) { return off != rptr.off; }

	bool is_null() { return !off; }
	void set_null() { off = 0; }
	static void set_base(PMEMoid o) { base_oid = o; base_address = (UCHAR*)pmemobj_direct(o); }
	static rel_ptr<T> null() { return rel_ptr<T>(); }
};

template<typename T>
UCHAR * rel_ptr<T>::base_address(nullptr);
template<typename T>
PMEMoid rel_ptr<T>::base_oid(OID_NULL);

#endif // !REL_PTR_H

/*
insert:
1) writer tests Frozen Bit, retraverse if failed
to avoid duplicate keys:
2) scan the sorted keys, fail if found a duplicate key
3) scan the unsorted keys
	3.2) fail if meet a duplicate key
	3.1) if meet a meta entry whose visiable unset and epoch = current global epoch, 
		set Retry bit and continue
4) reserve space in meta entry and block
	by adding one to record count and adding data length to node block size(both in node status)
	and flipping meta entry offset's high bit and setting the rest bits to current index global epoch
	4.1) in case fail => concurrent thread may be trying to insert duplicate key,
		so need to set Recheck flag
5) unset visiable bit and copy data to block
6) persist
6.5) re-scan prior positions if Recheck is set
	6.5.1) if find a duplicate key, set offset and block = 0
	return fail
7) 2-word PMwCAS:
	set status back => Frozen Bit isn't set
	set meta entry to visiable and correct offset
	7.1) if fails and Frozen bit is not set, read status and retry
	7.2) if Frozen bit is set, abort and retry the insert
*/

/*
delete:
1) 2-word PMwCAS on
	meta entry: visiable = 0, offset = 0
	node status: delete size += data length
	1.1) if fails due to Frozen bit set, abort and retraverse
	1.2) otherwise, read and retry
*/

/*
update(swap pointer):
1) 3-word PMwCAS
	pointer in storage block
	meta entry status => competing delete
	node status => Frozen bit
*/

/*
read
1) binary search on sorted keys
2) linear scan on unsorted keys
3) return the record found
*/

/*
range scan: [beg_key, end_key)
one leaf node at a time
1) enter the epoch
2) construct a response_array(visiable and not deleted)
3) exit epoch
4) one-record-at-a-time
5) enter new epoch
6) greater than search the largest key in response array
*/

/*
consolidate:

trigger by
	either free space too small or deleted space too large

1) single-word PMwCAS on status => frozen bit
2) scan all valid records(visiable and not deleted)
	calculate node size = record size + free space
	2.1) if node size too large, do a split
	2.2) otherwise, allocate a new node N', copy records sorted, init header
3) use path stack to find Node's parent P
	3.1) if P's frozen, retraverse to locate P
4) 2-word PMwCAS
	r in P that points to N => N'
	P's status => detect concurrent freeze
5) N is ready for gc
6) 
*/
package godb

import (
	"fmt"
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {
	// return 0, fmt.Errorf("computeFieldSum not implemented") // replace me
	fname := "./test1.dat"
	hp , err := NewHeapFile(fname,&td,bp)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	err = hp.LoadFromCSV(f, true, ",", false)
	if err != nil {
		return 0, err
	}
	fieldIndex := -1
	for i, field := range td.Fields {
		if field.Fname == sumField {
			if field.Ftype != IntType {
				return 0, fmt.Errorf("field %s is not an integer field", sumField)
			}
			fieldIndex = i
			break
		}
	}
	if fieldIndex == -1 {
		return 0, fmt.Errorf("field %s not found", sumField)
	}
	iter, err := hp.Iterator(-1)
	if err != nil {
		return 0, err
	}
	var sum int64 = 0
	for {
		tup, err := iter()
		if err != nil {
			return 0, err
		}
		if tup == nil {
			break
		}
		field := tup.Fields[fieldIndex]
		value := field.(IntField).Value
		sum += value
	}
	return int(sum), nil
}
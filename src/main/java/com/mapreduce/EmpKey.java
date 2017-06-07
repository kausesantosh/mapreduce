package com.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class EmpKey implements WritableComparable<EmpKey> {
	
	
	private int empId;
	private int sal;
	
	public EmpKey(){
		
	}
	
	public EmpKey(int empId, int sal) {
		super();
		this.empId = empId;
		this.sal = sal;
	}

	public int getEmpId() {
		return empId;
	}

	public void setEmpId(int empId) {
		this.empId = empId;
	}

	public int getSal() {
		return sal;
	}

	public void setSal(int sal) {
		this.sal = sal;
	}

	 public void write(DataOutput out) throws IOException {
		    out.writeInt(empId);
		    out.writeInt(sal);
		  }

		  @Override
		  public void readFields(DataInput in) throws IOException {
		    empId = in.readInt();
		    sal = in.readInt();
		  }
		  
		  @Override
		  public int hashCode() {
		    return empId * 163 + sal;
		  }
		  
		  @Override
		  public boolean equals(Object o) {
		    if (o instanceof EmpKey) {
		      EmpKey ip = (EmpKey) o;
		      return empId == ip.empId && sal == ip.sal;
		    }
		    return false;
		  }

		  @Override
		  public String toString() {
		    return empId + "\t" + sal;
		  }
		  
		  @Override
		  public int compareTo(EmpKey emp) {
		    int cmp = compare(empId, emp.empId);
		    if (cmp != 0) {
		      return cmp;
		    }
		    return compare(sal, emp.sal);
		  }
		  
		  /**
		   * Convenience method for comparing two ints.
		   */
		  public static int compare(int a, int b) {
		    return (a < b ? -1 : (a == b ? 0 : 1));
		  }
}

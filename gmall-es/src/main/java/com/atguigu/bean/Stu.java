package com.atguigu.bean;

public class Stu {

    private int stu_id;
    private String name;

    public Stu() {
    }

    public int getStu_id() {
        return stu_id;
    }

    public void setStu_id(int stu_id) {
        this.stu_id = stu_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Stu stu = (Stu) o;

        if (stu_id != stu.stu_id) return false;
        return name != null ? name.equals(stu.name) : stu.name == null;
    }

    @Override
    public int hashCode() {
        int result = stu_id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Stu{" +
                "stu_id=" + stu_id +
                ", name='" + name + '\'' +
                '}';
    }
}

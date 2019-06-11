package com.sovince.pojo;

/**
 * Created by vince
 * Email: so_vince@outlook.com
 * Data: 2019/6/10
 * Time: 21:50
 * Description:
 */
public class StuPojo {
    private Integer id;
    private String name;
    private Integer age;

    public StuPojo() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "StuPojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}

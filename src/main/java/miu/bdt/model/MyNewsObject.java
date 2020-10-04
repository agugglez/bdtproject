package miu.bdt.model;

import java.io.Serializable;

public class MyNewsObject implements Serializable {
    private static final long serialVersionUID = -2375635104087226938L;

    private String id;
    private String title;
    private String text;

    public MyNewsObject(String id, String title, String text){
        this.id = id;
        this.title = title;
        this.text = text;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "MyNewsObject{" +
                "title='" + title + '\'' +
                '}';
    }
}

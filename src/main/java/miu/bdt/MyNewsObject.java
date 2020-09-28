package miu.bdt;

public class MyNewsObject {
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
}

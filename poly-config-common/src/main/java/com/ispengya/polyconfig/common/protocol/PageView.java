package com.ispengya.polyconfig.common.protocol;

import java.util.List;

/**
 * 分页视图，用于列表类 HTTP 响应。
 *
 * @param <T> 条目类型
 */
public class PageView<T> {

    /**
     * 当前页条目。
     */
    private List<T> items;

    /**
     * 过滤后的总条数。
     */
    private int total;

    /**
     * 当前页码（从 1 开始）。
     */
    private int page;

    /**
     * 每页条数。
     */
    private int size;

    public PageView() {
    }

    /**
     * 创建分页视图。
     *
     * @param items 当前页条目
     * @param total 总条数
     * @param page 页码
     * @param size 每页条数
     */
    public PageView(List<T> items, int total, int page, int size) {
        this.items = items;
        this.total = total;
        this.page = page;
        this.size = size;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}

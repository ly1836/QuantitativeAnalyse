package com.ly.w;

import javax.swing.*;
import java.awt.*;

public class Test extends JPanel {
    private double x;
    private double y;

    @Override
    protected void paintComponent(Graphics g) {
        // TODO Auto-generated method stub
        super.paintComponent(g);
        g.setColor(Color.WHITE);//设置面板背景色
        g.fillRect(0, 0, 580, 300);//填充面板
        g.setColor(Color.RED);//设置画线的颜色
        double maxY = 0d;
        int countMaxY = 0;
        //一个周期
        for (x = 150; x <= 490; x += 0.01){
            //转化为弧度,1度=π/180弧度
            y = Math.sin(x * Math.PI / 90);
            if(y > maxY){
                maxY = y;
            }
            if(y == 1.0){
                countMaxY++;
            }
            System.out.println("y: " + y);
            //便于在屏幕上显示
            y = (100 + 80 * y);
            //g.drawString(".",(int)x,(int)y);//用这种方式也可以
            //画点
            g.drawLine((int) x, (int) y, (int) x, (int) y);
        }
        System.out.println("maxY: " + maxY);
        System.out.println("countMaxY: " + countMaxY);
    }

    public static void main(String[] args) {
        Test s = new Test();
        JFrame j = new JFrame();
        j.setTitle("一个周期的正弦曲线");
        j.add(s);
        j.setSize(600, 300);
        j.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        j.setVisible(true);
    }
}

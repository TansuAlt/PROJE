package org.example;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try {
            String jdbcUrl = "jdbc:mysql://localhost:3306/new_schema?useSSL=false";
            String username = "root";
            String password = "tansu";

            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

            File xmlFile = new File("C:\\Users\\Tansu\\IdeaProjects\\PROJE\\src\\operators.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList companyList = doc.getElementsByTagName("Company");

            for (int i = 0; i < companyList.getLength(); i++) {
                Element company = (Element) companyList.item(i);

                String companyName = company.getElementsByTagName("CompanyName").item(0).getTextContent();
                String route = company.getElementsByTagName("Route").item(0).getTextContent();

                NodeList blockList = company.getElementsByTagName("Block");

                for (int j = 0; j < blockList.getLength(); j++) {
                    Element block = (Element) blockList.item(j);

                    String ndc = block.getElementsByTagName("NDC").item(0).getTextContent();
                    String prefix = block.getElementsByTagName("Prefix").item(0).getTextContent();
                    String size = block.getElementsByTagName("Size").item(0).getTextContent();

                    insertIntoDatabase(connection, companyName, route, ndc, prefix, size);
                }
            }

            connection.close();

        } catch (ParserConfigurationException | IOException | SAXException | SQLException e) {
            System.err.println("Veritabanına veri eklenirken hata oluştu:");
            e.printStackTrace();
        }
    }

    private static void insertIntoDatabase(Connection connection, String companyName, String route, String ndc, String prefix, String size) throws SQLException {
        // mysql için INSERT sorgusu
        String sql = "INSERT INTO new_schema.prefix_tanimlari (operator, prefix, operator_kodu) VALUES (?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, companyName); // operator sütunu için companyName kullanıldı
        statement.setString(2, prefix);
        statement.setInt(3, Integer.parseInt(ndc));

        // Sorguyu çalıştır
        statement.executeUpdate();
        statement.close();
    }
}

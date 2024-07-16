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
import java.sql.*;

public class Main {
    public static void main(String[] args) {
        try {
            String jdbcUrl = "jdbc:mysql://localhost:3306/new_schema?useSSL=false";
            String username = "root";
            String password = "tansu";

            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            System.out.println("Connection is successful to the database " + jdbcUrl);

            // İlk dosyayı parse et ve veritabanına ekle
            File xmlFile1 = new File("C:\\Users\\Tansu\\IdeaProjects\\PROJE\\src\\NumberPlanOld.xml");
            parseAndInsertXML(connection, xmlFile1);

            // İkinci dosyayı parse et ve karşılaştır
            File xmlFile2 = new File("C:\\Users\\Tansu\\IdeaProjects\\PROJE\\src\\NumberPlanNew.xml");
            parseAndCompareXML(connection, xmlFile2);

            connection.close();

        } catch (ParserConfigurationException | IOException | SAXException | SQLException e) {
            System.err.println("Veritabanına veri eklenirken hata oluştu:");
            e.printStackTrace();
        }
    }

    private static void parseAndInsertXML(Connection connection, File xmlFile) throws ParserConfigurationException, IOException, SAXException, SQLException {
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
    }

    private static void insertIntoDatabase(Connection connection, String companyName, String route, String ndc, String prefix, String size) throws SQLException {
        String sql = "INSERT INTO new_schema.prefix_tanimlari (operator, prefix, operator_kodu) VALUES (?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, companyName);
        statement.setString(2, prefix);
        statement.setInt(3, Integer.parseInt(ndc));
        statement.executeUpdate();
        statement.close();
    }

    private static void parseAndCompareXML(Connection connection, File xmlFile) throws ParserConfigurationException, IOException, SAXException, SQLException {
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

                compareAndUpdateDatabase(connection, companyName, route, ndc, prefix, size);
            }
        }
    }

    private static void compareAndUpdateDatabase(Connection connection, String companyName, String route, String ndc, String prefix, String size) throws SQLException {
        // Veritabanındaki veriler ile karşılaştır ve gerekli aksiyonları al
        String sqlCheck = "SELECT prefix FROM new_schema.prefix_tanimlari WHERE operator = ? AND operator_kodu = ?";
        PreparedStatement statementCheck = connection.prepareStatement(sqlCheck);
        statementCheck.setString(1, companyName);
        statementCheck.setInt(2, Integer.parseInt(ndc));
        ResultSet resultSet = statementCheck.executeQuery();

        if (resultSet.next()) {
            String dbPrefix = resultSet.getString("prefix");

            if (!dbPrefix.equals(prefix)) {
                // Prefix değişmiş, güncelle
                updateDatabase(connection, companyName, ndc, prefix);
                System.out.println("Prefix güncellendi: " + dbPrefix + " -> " + prefix);
            }
        } else {
            // Veritabanında yoksa, ekle
            insertIntoDatabase(connection, companyName, route, ndc, prefix, size);
            System.out.println("Yeni kayıt eklendi: " + companyName + ", " + prefix);
        }

        resultSet.close();
        statementCheck.close();
    }

    private static void updateDatabase(Connection connection, String companyName, String ndc, String newPrefix) throws SQLException {
        String sqlUpdate = "UPDATE new_schema.prefix_tanimlari SET prefix = ? WHERE operator = ? AND operator_kodu = ?";
        PreparedStatement statementUpdate = connection.prepareStatement(sqlUpdate);
        statementUpdate.setString(1, newPrefix);
        statementUpdate.setString(2, companyName);
        statementUpdate.setInt(3, Integer.parseInt(ndc));
        statementUpdate.executeUpdate();
        statementUpdate.close();
    }
}

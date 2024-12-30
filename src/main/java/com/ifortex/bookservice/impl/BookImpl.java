package com.ifortex.bookservice.impl;

import com.ifortex.bookservice.service.BookService;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class BookImpl implements BookService {

    @Override
    public Map<String, Long> getBooks() {
        SortedMap<String , Long> resMap = new TreeMap<>();
        try {
            Connection connection  = DriverManager.getConnection("jdbc:postgresql://localhost:5432/book_service",
                    "book_service","book_service");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select n, count(n) as count_n\n" +
                    "  from books\n" +
                    "  cross join unnest (genre) t(n)\n" +
                    "group by n\n" +
                    "order by count_n desc");
            while (resultSet.next()){
                resMap.put(resultSet.getString(1),resultSet.getLong(2));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return resMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));
    }
}

package com.ifortex.bookservice.impl;

import com.ifortex.bookservice.model.Book;
import com.ifortex.bookservice.model.Member;
import com.ifortex.bookservice.service.MemberService;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class MemberImpl implements MemberService {
    @Override
    public Member findMember() {
        Member member = new Member();
        try {
            Connection connection  = DriverManager.getConnection("jdbc:postgresql://localhost:5432/book_service",
                    "book_service","book_service");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select members.id, members.name, members.membership_date from members inner join member_books ON members.id = member_books.member_id\n" +
                    "inner join books ON member_books.book_id = books.id\n" +
                    "\n" +
                    "where 'Romance' = ANY(genre) Order By publication_date ASC, membership_date DESC limit 1");
            resultSet.next();
            member.setId(resultSet.getLong(1));
            member.setName(resultSet.getString(2));
            member.setMembershipDate(resultSet.getTimestamp(3).toLocalDateTime());
            member.setBorrowedBooks(new ArrayList<>());
            resultSet = statement.executeQuery("select * from books left join member_books ON member_books.book_id = books.id \n" +
                    "where member_id = "+member.getId());
            while (resultSet.next()){
                Book book = new Book();
                book.setId(resultSet.getLong(1));
                book.setTitle(resultSet.getString(2));
                book.setDescription(resultSet.getString(3));
                book.setAuthor(resultSet.getString(4));
                book.setPublicationDate(resultSet.getTimestamp(5).toLocalDateTime());
                book.setGenres(Arrays.stream(((String[])resultSet.getArray(6).getArray())).collect(Collectors.toSet()));
                member.getBorrowedBooks().add(book);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return member;
    }

    @Override
    public List<Member> findMembers() {
        List<Member> ans = new ArrayList<>();
        try {
            Connection connection  = DriverManager.getConnection("jdbc:postgresql://localhost:5432/book_service",
                    "book_service","book_service");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from members left join member_books on members.id=member_books.member_id\n" +
                    "where member_id is null and membership_date between '2023-01-01 00:00:00' and '2023-12-31 23:59:59'");
            while (resultSet.next()){
                Member member = new Member();
                member.setId(resultSet.getLong(1));
                member.setName(resultSet.getString(2));
                member.setMembershipDate(resultSet.getTimestamp(3).toLocalDateTime());
                member.setBorrowedBooks(new ArrayList<>());
                ans.add(member);
            };
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ans;
    }
}

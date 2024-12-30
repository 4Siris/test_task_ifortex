package com.ifortex.bookservice.controller;

import com.ifortex.bookservice.service.BookService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

// Attention! It is FORBIDDEN to make any changes in this file!
@RestController
@RequestMapping("api/v1/books")
@RequiredArgsConstructor
public class BookController {

  private final BookService bookService;

  @GetMapping("statistic")
  public Map<String, Long> getStatistic() {
    return bookService.getBooks();
  }
}

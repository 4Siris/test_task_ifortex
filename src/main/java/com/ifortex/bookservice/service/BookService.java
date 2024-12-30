package com.ifortex.bookservice.service;

import com.ifortex.bookservice.model.Book;

import java.util.Map;

// Attention! It is FORBIDDEN to make any changes in this file!
public interface BookService {
  /**
   * Returns the count of the {@link Book}s by each genre, ordered from the genre with the most
   * books to the least
   *
   * @return desired result as a Map where key is book genre and value is count of the books
   */
  Map<String, Long> getBooks();
}

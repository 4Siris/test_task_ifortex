Tech Task Requirements

Installed tools requirements:
Java 17,
Maven,
Any IDE,
Docker.

In the project, you will find a docker-compose package. Run docker compose up to start the services. During the first application startup, database migration will execute and populate the necessary data.
Restart the container if any troubles with connecting appears.

Overview

You are working on a book service platform and are required to implement two services: BookService and MemberService. Please note that certain classes have restrictions and cannot be edited.

Tasks:
* BookService Implementation.

Implement the BookService interface and place your implementation in the impl package.
      Required Method:
  1. Count books by genre.

    Implement a method that retrieves the total count of books for each genre, ordered from the genre with the most books to the least.
* MemberService Implementation.

Implement the MemberService interface and place your implementation in the impl package.
      Required Methods:
  1. Member search.

    Implement a method that finds and returns the member who has read the oldest “Romance” genre book and who was most recently registered on the platform.
  2. Members search.

    Implement a method that finds and returns members who registered in 2023 but have not read any books.


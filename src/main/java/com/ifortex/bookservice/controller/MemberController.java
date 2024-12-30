package com.ifortex.bookservice.controller;

import com.ifortex.bookservice.model.Member;
import com.ifortex.bookservice.service.MemberService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// Attention! It is FORBIDDEN to make any changes in this file!
@RestController
@RequestMapping("api/v1/members")
@RequiredArgsConstructor
public class MemberController {

  private final MemberService memberService;

  @GetMapping("amateur")
  public Member findMember() {
    return memberService.findMember();
  }

  @GetMapping
  public List<Member> findMembers() {
    return memberService.findMembers();
  }
}

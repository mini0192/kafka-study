package com.chatting.kafkatest;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TestDto implements Serializable {
    Long id;
    String username;
    String nickname;
}

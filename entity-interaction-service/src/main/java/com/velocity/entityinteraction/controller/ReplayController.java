package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.service.ReplayService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/replay")
    public ReplayService.ReplayResult replay(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) Integer limit) {
        if (userId != null) {
            return replayService.replayUser(userId);
        }
        return replayService.replayAll(limit);
    }
}

package io.youngwon.api.api

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class TicketApiController {

    @GetMapping("/api/v1/tickets")
    public fun getTicket(): TicketResponse {
        return TicketResponse(id = "abc", isSuccess = true)
    }
}

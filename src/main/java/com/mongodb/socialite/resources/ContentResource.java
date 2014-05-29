package com.mongodb.socialite.resources;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.services.ContentService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/content")
@Produces(MediaType.APPLICATION_JSON)
public class ContentResource {

    private final ContentService content;

    public ContentResource(ContentService content) {
        this.content = content;
    }

    @GET
    @Path("/{content_id}")
    public Content get(@PathParam("content_id") String content_id ) {

        ContentId id = new ContentId(content_id);
        return this.content.getContentById(id);
    }

}

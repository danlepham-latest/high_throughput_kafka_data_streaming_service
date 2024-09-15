package com.danlepham.high_throughput_kafka_data_streaming_service.util;

import io.quarkus.mailer.Mail;
import io.quarkus.mailer.Mailer;
import io.quarkus.mailer.reactive.ReactiveMailer;
import io.smallrye.mutiny.Uni;
import lombok.NoArgsConstructor;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@NoArgsConstructor
@ApplicationScoped
public class SmtpEmailService {

    private Mailer mailer;
    private ReactiveMailer reactiveMailer;

    @ConfigProperty(name = "quarkus.mailer.enabled", defaultValue = "false")
    boolean mailerEnabled;
    @ConfigProperty(name = "quarkus.mailer.to")
    String recipients;
    @ConfigProperty(name = "quarkus.mailer.toDelimiter", defaultValue = ";")
    String toDelimiter;

    private String[] recipients_;

    @Inject
    public SmtpEmailService(Mailer mailer, ReactiveMailer reactiveMailer) {
        this.mailer = mailer;
        this.reactiveMailer = reactiveMailer;
    }
    //

    @PostConstruct
    void parseRecipients() {
        this.recipients_ = recipients.split(toDelimiter);
    }

    public void sendEmailText(String subject, String emailText) {
        if (mailerEnabled) {
            mailer.send(buildErrorEmail(subject, emailText));
        }
    }

    public Uni<Void> sendEmailTextAsync(String subject, String emailText) {
        if (mailerEnabled) {
            return reactiveMailer.send(buildErrorEmail(subject, emailText));
        }
        return Uni.createFrom().nullItem();
    }

    private Mail buildErrorEmail(String subject, String emailText) {
        // set subject headline and body text
        Mail mail = new Mail();
        mail.setSubject(subject);
        mail.setText(emailText);

        // append all recipients from the list to the "email to" field
        mail.addTo(recipients_);

        return mail;
    }
}
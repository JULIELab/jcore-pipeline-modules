package de.julielab.jcore.pipeline.builder.base.connectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URL;

public class RepositoryBranchInformation {
    private String name;
    private Commit commit;
    @JsonProperty("protected")
    private boolean prot;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Commit getCommit() {
        return commit;
    }

    public void setCommit(Commit commit) {
        this.commit = commit;
    }

    public boolean isProt() {
        return prot;
    }

    public void setProt(boolean prot) {
        this.prot = prot;
    }

    @Override
    public String toString() {
        return "RepositoryBranchInformation{" +
                "name='" + name + '\'' +
                ", commit=" + commit +
                ", prot=" + prot +
                '}';
    }

    public static class Commit {
        private String sha;
        private URL url;

        public String getSha() {
            return sha;
        }

        public void setSha(String sha) {
            this.sha = sha;
        }

        public URL getUrl() {
            return url;
        }

        public void setUrl(URL url) {
            this.url = url;
        }

        @Override
        public String toString() {
            return "Commit{" +
                    "sha='" + sha + '\'' +
                    ", url=" + url +
                    '}';
        }
    }
}

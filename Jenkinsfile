#!groovy

node("executor") {
    checkout scm

    def commitHash  = sh(returnStdout: true, script: 'git rev-parse HEAD | cut -c-7').trim()
    def imageTag = "${env.BUILD_NUMBER}-${commitHash}"

    def sbt = "sbt -Dsbt.log.noformat=true -Dversion=$imageTag"
    def pennsieveNexusCreds = usernamePassword(
        credentialsId: "pennsieve-nexus-ci-login",
        usernameVariable: "PENNSIEVE_NEXUS_USER",
        passwordVariable: "PENNSIEVE_NEXUS_PW"
    )

    stage("Build") {
        withCredentials([pennsieveNexusCreds]) {
            sh "$sbt clean +compile"
        }
    }

    stage("Test") {
        withCredentials([pennsieveNexusCreds]) {
            try {
                sh "$sbt coverageOn +test"
            } finally {
                junit '**/target/test-reports/*.xml'
            }
        }
    }

    stage("Test Coverage") {
        withCredentials([pennsieveNexusCreds]) {
            sh "$sbt +coverageReport"
        }
    }

    if (env.BRANCH_NAME == "main") {
        stage("Publish") {
            withCredentials([pennsieveNexusCreds]) {
                sh "$sbt +client/publish"
            }
        }

        stage("Docker") {
            withCredentials([pennsieveNexusCreds]) {
                sh "$sbt clean server/docker syncElasticSearch/docker"
            }

            sh "docker tag pennsieve/discover-service:latest pennsieve/discover-service:$imageTag"
            sh "docker push pennsieve/discover-service:latest"
            sh "docker push pennsieve/discover-service:$imageTag"

            sh "docker tag pennsieve/discover-service-sync-elasticsearch:latest pennsieve/discover-service-sync-elasticsearch:$imageTag"
            sh "docker push pennsieve/discover-service-sync-elasticsearch:latest"
            sh "docker push pennsieve/discover-service-sync-elasticsearch:$imageTag"
        }

        try {
            stage("Deploy") {
                build job: "service-deploy/pennsieve-non-prod/us-east-1/dev-vpc-use1/dev/discover-service",
                    parameters: [
                    string(name: 'IMAGE_TAG', value: imageTag),
                    string(name: 'TERRAFORM_ACTION', value: 'apply')
                ]
            }
        } finally {
            stage("E2E") {
                build job: "pennsieve/end-to-end-tests/master"
            }
        }
    }
}

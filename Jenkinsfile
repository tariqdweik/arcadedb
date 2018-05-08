/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

#!/usr/bin/env groovy

node {
    try {

        stage('checkout') {
            checkout scm
        }

        stage('check java') {
            sh "java -version"
        }



        stage('build') {
            try {
                sh "./mvnw clean install"
            } catch (err) {
                throw err
            } finally {
                junit '**/surefire-reports/**/*.xml'
            }
        }

        slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    } catch (e) {
        currentBuild.result = 'FAILURE'
        slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
        throw e
    }

}

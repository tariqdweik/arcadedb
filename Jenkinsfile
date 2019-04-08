#!/usr/bin/env groovy

/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */


node {
    try {
        properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '10', artifactNumToKeepStr: '10', daysToKeepStr: '10', numToKeepStr: '10')), disableConcurrentBuilds()])

        cleanWs()

        stage('checkout') {
            checkout scm
        }

        docker.image('openjdk:8-jdk').inside('-v  /home/player/volumes/jenkins_home/.m2:/.m2"') {


            stage('check java') {
                sh "java -version"
            }

            stage('build') {
                try {
                    sh "./mvnw -fae clean install"
                } catch (err) {
                    throw err
                } finally {
                    junit '**/surefire-reports/**/*.xml'
                    step([$class       : 'JacocoPublisher',
                          execPattern  : '**/**.exec',
                          classPattern : '**/classes',
                          sourcePattern: '**/src/main/java'])
                }
            }
        }
        googlechatnotification url: 'id:chat_jenkins_id', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})"
    } catch (e) {
        currentBuild.result = 'FAILURE'
        googlechatnotification url: 'id:chat_jenkins_id', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}"
        throw e
    }

}

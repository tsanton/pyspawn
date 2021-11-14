#!/bin/bash
py.test --junitxml=xunit-reports/xunit-result-all.xml \
        --cov \
        --cov-report=xml:coverage-reports/coverage.xml \
        --cov-report=html:coverage-reports/
# -*- coding: utf8 -*-
import os
import sys
import nose


# Set up the environment for our test project.
ROOT = os.path.abspath(os.path.dirname(__file__))

# import to check for the existence of Django

os.environ.update({'DJANGO_SETTINGS_MODULE': 'test_settings'})
sys.path.insert(0, ROOT)

# This can't be imported until after we've fiddled with the
# environment.
from django.test.utils import setup_test_environment

setup_test_environment()

# Run nose.
#
sys.exit(nose.main())

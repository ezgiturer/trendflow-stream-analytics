FROM bitnami/spark:3.5.0

USER root

# Install redis-py
# Use pip3 to ensure it's installed for Python 3
RUN pip3 install redis==5.0.1 && \
    # Optional: Clean up pip cache to reduce image size
    rm -rf /root/.cache/pip

# Switch back to the non-root user (important for security best practices)
USER 1001
FROM node:22-slim AS frontend-build
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
COPY templates/ /templates/
COPY static/css/ /static/css/
COPY static/js/index.js static/js/app-shell.js /static/js/
RUN npm run build

FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . /app
COPY --from=frontend-build /static/react /app/static/react
COPY --from=frontend-build /static/css/legacy-utilities.css /app/static/css/legacy-utilities.css
COPY --from=frontend-build /static/fonts /app/static/fonts

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# Use $PORT if provided by platform; default to 8000 locally
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]

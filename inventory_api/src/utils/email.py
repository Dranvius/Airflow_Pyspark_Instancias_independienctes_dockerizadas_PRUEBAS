import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- Configuración del servidor SMTP desde variables de entorno ---
SMTP_HOST = os.environ.get("SMTP_HOST")  # p.ej., "smtp.gmail.com"
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
EMAIL_SENDER = os.environ.get("EMAIL_SENDER", SMTP_USER)

def send_email(recipient: str, subject: str, body: str) -> bool:
    """
    Envía un correo electrónico utilizando la configuración definida en las variables de entorno.

    Args:
        recipient: El destinatario del correo.
        subject: El asunto del correo.
        body: El cuerpo del correo (puede ser HTML).

    Returns:
        True si el correo se envió con éxito, False en caso contrario.
    """
    if not all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD]):
        print(" Faltan variables de entorno para la configuración de SMTP. No se puede enviar el correo.")
        return False

    # --- Construir el mensaje ---
    message = MIMEMultipart()
    message["From"] = EMAIL_SENDER
    message["To"] = recipient
    message["Subject"] = subject
    message.attach(MIMEText(body, "html")) # Se asume cuerpo en HTML para más flexibilidad

    # --- Enviar el correo ---
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # Habilitar seguridad
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(EMAIL_SENDER, recipient, message.as_string())
            print(f"✅ Correo enviado exitosamente a {recipient}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("❌ Error de autenticación SMTP. Revisa el usuario y la contraseña.")
        return False
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")
        return False

# --- Ejemplo de uso (para pruebas) ---
if __name__ == '__main__':
    print("Ejecutando prueba de envío de correo...")
    # Para probar, asegúrate de tener las variables de entorno SMTP_* configuradas.
    # Por ejemplo, puedes exportarlas en tu terminal antes de correr:
    # export SMTP_HOST=smtp.example.com
    # export SMTP_PORT=587
    # ... etc.
    
    test_recipient = "test@example.com"
    test_subject = "Alerta de Prueba: Stock Bajo"
    test_body = """
    <html>
    <body>
        <h1>Alerta de Inventario</h1>
        <p>Este es un correo de prueba.</p>
        <p>El producto <b>XYZ-123</b> tiene un stock bajo.</p>
        <p>Cantidad actual: <b>5</b></p>
    </body>
    </html>
    """
    
    # Comprobar si las variables de entorno están configuradas antes de intentar enviar
    if all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD]):
        send_email(test_recipient, test_subject, test_body)
    else:
        print("\nPrueba omitida: Debes configurar las variables de entorno SMTP para probar el envío de correo.")


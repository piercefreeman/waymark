import type { ReactNode } from 'react';

export const metadata = {
  description: 'Waymark JavaScript example app',
  title: 'Waymark JS Example'
};

export default function RootLayout({
  children
}: Readonly<{
  children: ReactNode;
}>) {
  return (
    <html lang="en">
      <body style={{ margin: 0 }}>{children}</body>
    </html>
  );
}
